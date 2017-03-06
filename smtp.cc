#include <arpa/inet.h>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

using namespace std;

// constant strings
const char* NEW_CONN 			 = "New connection\r\n";
const char* SERVICE_READY 		 = "220 localhost service ready\r\n";
const char* SERVICE_CLOSING		 = "221 localhost service closing transmission channel\r\n";
const char* HELO_RESPONSE 		 = "250 localhost\r\n";
const char* OK 					 = "250 OK\r\n";
const char* START_MAIL 			 = "354 Start mail input; end with <CRLF>.<CRLF>\r\n";
const char* SERVICE_UNAVAILABLE  = "421 localhost service not available, closing transmission channel\r\n";
const char* UNRECGONIZED_COMMAND = "500 Syntax error, command unrecognized\r\n";
const char* SYNTAX_ERROR 		 = "501 Syntax error in parameters or arguments\r\n";
const char* BAD_SEQUENCE 		 = "503 Bad sequence of commands\r\n";
const char* MAILBOX_UNAVAILABLE  = "550 Requested action not taken: mailbox unavailable\r\n";
const char* CLOSE_CONN 			 = "Connection closed\r\n";

// constant integers
const int BUFFER_SIZE 	= 16384;
const int COMMAND_LEN 	= 4;
const int RESPONSE_LEN 	= 128;
const int MAILBOX_LEN 	= 64;

// global variables
vector< pthread_t > THREADS;
vector< int > SOCKETS;
char* PARENTDIR;
unordered_set< string > MAILBOXES;
bool DEBUG = false;

// function signatures
void signal_handler(int arg);
void get_mailboxes();
void* worker(void* arg);
void handle_helo(int comm_fd, int* state, char* buffer, char* response);
void handle_mail(int comm_fd, int* state, char* buffer, char* sender, char* response);
void handle_rcpt(int comm_fd, int* state, char* buffer, vector< string >& rcpts, char* response);
void handle_data(int comm_fd, int* state, bool* is_data, char* buffer, char* end, string& content, 
	char* sender, vector< string >& rcpts, char* response);
void handle_noop(int comm_fd, int* state, char* response);
void handle_rset(int comm_fd, int* state, string& content, char* sender, vector < string >&rcpts, 
	char* response);
void handle_quit(int comm_fd, int* state, bool* quit, char* response);
void copy_mailbox(char* dest, char* src);
void copy_rcpt_host(char* rcpt, char* host, char* src);
void remove_command(char* buf, char* end);

// Main function of the program. Also the dispatcher of worker threads. This function parses command line 
// arguments, set up the server, and dispatches worker threads to handle connections.
int main(int argc, char *argv[]) {
	// signal handler
	signal(SIGINT, signal_handler);

	int option = 0;
	// port defaults to 2500 if no arguments given
	unsigned short port = 2500;

	while ((option = getopt(argc, argv, "p:av")) != -1) {
		switch(option) {
		case 'p':
			port = atoi(optarg);
			break;

		case 'a':
			cerr << "Name: Han Zhu\r\n" << "SEAS Login: zhuhan\r\n";
			exit(1);

		case 'v':
			DEBUG = true;
			break;

		default:
			cerr << "Usage: " << argv[0] << " [-p port number] [-a] [-v] [mailbox directory]\r\n";
			exit(1);
		}
	}

	// if no mailbox directory given
	if (optind == argc) {
		cerr << "Usage: " << argv[0] << " [-p port number] [-a] [-v] [mailbox directory]\r\n";
		exit(1);
	}
	PARENTDIR = (char*)malloc(sizeof(char*));
	strcpy(PARENTDIR, argv[optind]);
	get_mailboxes();

	// connect to socket
	int listen_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (listen_fd < 0) {
		cerr << "Error opening socket\r\n";
		exit(1);
	}

	SOCKETS.push_back(listen_fd);
	
	// set port as reusable
	const int enable = 1;
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

	// set up server
	struct sockaddr_in servaddr;
	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htons(INADDR_ANY);
	servaddr.sin_port = htons(port);

	bind(listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr));
	listen(listen_fd, 100);

	while (true) {
		// set up client connection
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);
		
		int fd = accept(listen_fd, (struct sockaddr*)&clientaddr, &clientaddrlen);
		if (fd == -1) {
			break;
		}
		SOCKETS.push_back(fd);

		if (DEBUG) {
			cerr << "[" << fd << "] " << NEW_CONN;
		}

		pthread_t thread;
		THREADS.push_back(thread);
		// dispatch worker thread to handle client communication
		pthread_create(&thread, NULL, &worker, &fd);
	}

	return 0;
}

// Handler of SIGINT. Once SIGINT is received, this function writes a message to clients, closes their 
// sockets, and terminates all working threads except main.
void signal_handler(int arg) {
	close(SOCKETS[0]);
	free(PARENTDIR);
	cout << "\r\n";

	for (int i = 1; i < SOCKETS.size(); i++) {
		write(SOCKETS[i], SERVICE_UNAVAILABLE, strlen(SERVICE_UNAVAILABLE));
		close(SOCKETS[i]);
		pthread_kill(THREADS[i - 1], 0);
	}
}

// Read the mailbox directory and add mailboxes to a hashset.
void get_mailboxes() {
	DIR* mbdir;
	struct dirent* entry;

	if ((mbdir = opendir(PARENTDIR)) != NULL) {
		int i = 0;
		while ((entry = readdir(mbdir)) != NULL) {
			if (i > 1) {
				string mailbox(entry->d_name);
				MAILBOXES.insert(mailbox);
			}
			i++;
		}
		closedir(mbdir);
	} else {
		cerr << "Mailbox directory does not exist\r\n";
		exit(1);
	}
}

// Worker thread that handles the connection. One thread for one client.
// arg: file descriptor of the socket the client connects to.
void* worker(void* arg) {
	int comm_fd = *(int*)arg;
	write(comm_fd, SERVICE_READY, strlen(SERVICE_READY));

	int state = 0;
	// 0 - just connected
	// 1 - HELO/RSET received
	// 2 - MAIL received
	// 3 - RCPT received
	// 4 - DATA received
	// 5 - DATA finished
	// 6 - QUIT received

	// buffers for client's command
	char buf[BUFFER_SIZE];
	char* curr = buf;
	bool quit = false;

	char sender[MAILBOX_LEN];
	vector< string > rcpts;
	bool is_data = false;
	string content;

	// into one connection
	while (!quit) {
		int curr_len = strlen(buf);
		int rlen = read(comm_fd, curr, BUFFER_SIZE - curr_len);
		char* end = (char*)malloc(sizeof(char*));

		// into one command - if command contains "<CR><LF>", enter loop
		while ((end = strstr(buf, "\r\n")) != NULL) {
			// move end to the end of "<CR><LF>"
			end += 2;

			char command[COMMAND_LEN];
			for (int i = 0; i < COMMAND_LEN; i++) {
				command[i] = buf[i];
			}

			char response[RESPONSE_LEN];

			// HELO response
			if (strcasecmp(command, "helo") == 0) {
				handle_helo(comm_fd, &state, buf, response);
			
			// MAIL response
			} else if (strcasecmp(command, "mail") == 0) {
				handle_mail(comm_fd, &state, buf, sender, response);
			
			// RCPT response
			} else if (strcasecmp(command, "rcpt") == 0) {
				handle_rcpt(comm_fd, &state, buf, rcpts, response);
			
			// DATA response
			} else if (strcasecmp(command, "data") == 0 || is_data) {
				handle_data(comm_fd, &state, &is_data, buf, end, content, sender, rcpts, response);

			// NOOP response
			} else if (strcasecmp(command, "noop") == 0) {
				handle_noop(comm_fd, &state, response);

			// RSET response
			} else if (strcasecmp(command, "rset") == 0) {
				handle_rset(comm_fd, &state, content, sender, rcpts, response);

			// QUIT response
			} else if (strcasecmp(command, "quit") == 0) {
				handle_quit(comm_fd, &state, &quit, response);

			// unknown command response
			} else {
				write(comm_fd, UNRECGONIZED_COMMAND, strlen(UNRECGONIZED_COMMAND));
				strcpy(response, UNRECGONIZED_COMMAND);
			}

			if (DEBUG) {
				fprintf(stderr, "[%d] C: %.*s", comm_fd, (int)(end - buf), buf);
				fprintf(stderr, "[%d] S: %s", comm_fd, response);
			}

			if (quit) {
				break;
			}

			// clear buffer of one full command
			remove_command(buf, end);
		}

		if (quit) {
			break;
		}

		// reset curr to point to the end of buffer
		curr = buf;
		while (*curr != '\0') {
			curr++;
		}
		free(end);
	}

	close(comm_fd);
	if (DEBUG) {
		cerr << "[" << comm_fd << "] " << CLOSE_CONN;
	}
	pthread_exit(NULL);
}

// Handler for HELO command. Checks whether the transaction is at the correct state and send response
// accordingly. Checks whether there is an argument after HELO. If there is not, a 501 is returned.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// response:	response written to client
void handle_helo(int comm_fd, int* state, char* buffer, char* response) {
	if (*state > 1) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else {
		string buf(buffer);
		buf.erase(buf.find_last_not_of(" \n\r\t") + 1);

		if (buf.length() <= 4) {
			write(comm_fd, SYNTAX_ERROR, strlen(SYNTAX_ERROR));
			strcpy(response, SYNTAX_ERROR);
		} else {
			write(comm_fd, HELO_RESPONSE, strlen(HELO_RESPONSE));
			*state = 1;
			strcpy(response, HELO_RESPONSE);
		}
	}
}

// Handler for MAIL command. Checks whether the transaction is at the correct state and send response
// accordingly. Copies the sender's email to a buffer.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// sender:		buffer to keep track of sender's email
// response:	response written to client
void handle_mail(int comm_fd, int* state, char* buffer, char* sender, char* response) {
	if (*state != 1) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else {
		copy_mailbox(sender, buffer);
		write(comm_fd, OK, strlen(OK));
		strcpy(response, OK);
		*state = 2;
	}
}

// Handler for RCPT command. Checks whether the transaction is at the correct state and send response
// accordingly. Checks whether the recipients exist. If so, copies their emails to a buffer.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// rcpts:		buffer to keep track of recipients
// response:	response written to client
void handle_rcpt(int comm_fd, int* state, char* buffer, vector< string >& rcpts, char* response) {
	if (*state < 2 || *state > 3) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else {
		char rcpt[MAILBOX_LEN];
		char host[MAILBOX_LEN];
		copy_rcpt_host(rcpt, host, buffer);
		string mbox(rcpt);
		mbox += ".mbox";

		if (strcmp(host, "localhost") != 0 || MAILBOXES.find(mbox) == MAILBOXES.end()) {
			write(comm_fd, MAILBOX_UNAVAILABLE, strlen(MAILBOX_UNAVAILABLE));
			strcpy(response, MAILBOX_UNAVAILABLE);
		} else {
			rcpts.push_back(mbox);
			write(comm_fd, OK, strlen(OK));
			strcpy(response, OK);

			*state = 3;
		}
	}
}

// Handler for DATA command. Checks whether the transaction is at the correct state and send response
// accordingly. Read client's message until <CR><LF>.<CR><LF> is received. When the full message is read,
// write it to recipients files and clear buffers.
// comm_fd: 	client's socket
// state: 		current transaction state
// is_data:		true if the message is not finished
// buffer:		master buffer for client's command
// end:			pointer to the end of one line in buffer
// content:		buffer to keep track of email message
// sender:		sender of the email
// rcpts:		recipients of the email
// response:	response written to client
void handle_data(int comm_fd, int* state, bool* is_data, char* buffer, char* end, string& content, 
	char* sender, vector< string >& rcpts, char* response) {

	if (*state < 3 || *state > 4) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else if (strcmp(buffer, ".\r\n") == 0) {
		*is_data = false;
		*state = 5;

		for (int i = 0; i < rcpts.size(); i++) {
			ofstream mbox;
			mbox.open(string(PARENTDIR) + "/" + rcpts[i], ios_base::app);

			time_t now = time(0);
			string timestamp = "From ";
			timestamp = timestamp + sender + " " + ctime(&now);

			mbox << timestamp << content;
			mbox.close();
		}

		content.clear();
		sender[0] = '\0';
		rcpts.clear();

		write(comm_fd, OK, strlen(OK));
		strcpy(response, OK);		
	} else if (!*is_data) {
		write(comm_fd, START_MAIL, strlen(START_MAIL));
		strcpy(response, START_MAIL);

		*is_data = true;
		*state = 4;
	} else {
		string line(buffer, end - buffer);
		content += line;
		response[0] = '\n';
		response[1] = '\0';
	}
}

// Handler for NOOP command. If the client already said HELO, reply with OK. If not, send an 503 error.
// comm_fd: 	client's socket
// state:		current transaction state
// response:	response written to client
void handle_noop(int comm_fd, int* state, char* response) {
	if (*state == 0) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else {
		write(comm_fd, OK, strlen(OK));
		strcpy(response, OK);
	}
}

// Handler for RSET command. Checks whether the transaction is at the correct state and send response
// accordingly. Clear all buffers and return to the state before transaction.
// comm_fd: 	client's socket
// state: 		current transaction state
// content:		email message
// sender:		sender of the email
// rcpts:		recipients of the email
// response:	response written to client
void handle_rset(int comm_fd, int* state, string& content, char* sender, vector < string >&rcpts, 
	char* response) {

	if (*state == 0) {
		write(comm_fd, BAD_SEQUENCE, strlen(BAD_SEQUENCE));
		strcpy(response, BAD_SEQUENCE);
	} else {
		content.clear();
		sender[0] = '\0';
		rcpts.clear();

		write(comm_fd, OK, strlen(OK));
		strcpy(response, OK);

		*state = 1;
	}
}

// Handler for QUIT command. Sets quit flag to true.
// comm_fd: 	client's socket
// state: 		current transaction state
// quit:		true if the client writes QUIT
// response:	response written to client
void handle_quit(int comm_fd, int* state, bool* quit, char* response) {
	*state = 6;
	*quit = true;

	write(comm_fd, SERVICE_CLOSING, strlen(SERVICE_CLOSING));
	strcpy(response, SERVICE_CLOSING);
}

// Parse an email address from src and copy it to dest.
// src: 	source buffer
// dest:	destination buffer
void copy_mailbox(char* dest, char* src) {
	int i = 0;
	while (src[i] != '<') {
		i++;
	}

	int j = 0;
	while (src[i] != '>') {
		// cout << src[i] << endl;
		dest[j] = src[i];
		i++;
		j++;
	}
	dest[j] = src[i];
}

// Parse recipient's email and host name from src and copy to rcpt and host.
// rcpt: 	buffer for recipient's email address
// host: 	buffer for host name
// src:		source buffer
void copy_rcpt_host(char* rcpt, char* host, char* src) {
	int i = 0;
	while (src[i] != '<') {
		i++;
	}
	i++;

	int j = 0;
	while (src[i] != '@') {
		rcpt[j] = src[i];
		i++;
		j++;
	}
	i++;

	j = 0;
	while (src[i] != '>') {
		host[j] = src[i];
		i++;
		j++;
	}
	host[j] = '\0';
}

// After worker thread has parsed a full command, this function clears the command from buffer, and moves
// all remaining characters to the start of buffer.
void remove_command(char* buf, char* end) {
	char* curr = buf;

	while (curr != end) {
		*curr = '\0';
		curr++;
	}

	curr = buf;

	while (*end != '\0') {
		*curr = *end;
		*end = '\0';
		curr++;
		end++;
	}
}
