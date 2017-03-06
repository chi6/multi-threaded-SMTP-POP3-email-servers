#include <arpa/inet.h>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <openssl/md5.h>
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
const char* SERVICE_READY 		 = "+OK POP3 ready [localhost]\r\n";
const char* NO_USER				 = "-ERR Sorry, no such mailbox here\r\n";
const char* USER_EXISTS 		 = "+OK Mailbox exists\r\n";
const char* INVALID_PASSWORD	 = "-ERR Invalid password\r\n";
const char* VALID_PASSWORD		 = "+OK Mailbox ready\r\n";
const char* NO_MESSAGE			 = "-ERR No such message\r\n";
const char* DELETED 			 = "+OK Message deleted\r\n";
const char* UNRECGONIZED_COMMAND = "-ERR Not supported\r\n";
const char* UIDL_ALL 			 = "+OK Unique-id listing follows\r\n";
const char* BAD_SEQUENCE 		 = "-ERR Bad sequence of commands\r\n";
const char* RESET 				 = "+OK Messages reset\r\n";
const char* SERVICE_UNAVAILABLE  = "-ERR Service not available, closing transmission channel\r\n";
const char* QUIT 				 = "+OK POP3 server signing off\r\n";
const char* CLOSE_CONN 			 = "Connection closed\r\n";

// constant integers
const int BUFFER_SIZE 	= 1024;
const int COMMAND_LEN 	= 4;
const int MAILBOX_LEN 	= 64;
const int AUTHORIZATION = 0;
const int TRANSACTION 	= 1;
const int UPDATE 		= 2;

// global variables
vector< pthread_t > THREADS;
vector< int > SOCKETS;
char* PARENTDIR;
unordered_set< string > MAILBOXES;
bool DEBUG = false;

// wrapper class for message
class Message {
public:
	string content;
	bool deleted;

public:
	Message(string content): content(content), deleted() {}
};

// function signatures
void signal_handler(int arg);
void get_mailboxes();
void* worker(void* arg);
void handle_user(int comm_fd, int* state, char* buffer, char* user);
void handle_pass(int comm_fd, int* state, char* buffer, char* user, vector< Message >& messages);
void handle_stat(int comm_fd, int* state, vector< Message >& messages);
void handle_list(int comm_fd, int* state, char* buffer, vector< Message >& messages);
void handle_uidl(int comm_fd, int* state, char* buffer, vector< Message >& messages);
void handle_retr(int comm_fd, int* state, char* buffer, vector< Message >& messages);
void handle_dele(int comm_fd, int* state, char* buffer, vector< Message >& messages);
void handle_noop(int comm_fd, int* state);
void handle_rset(int comm_fd, int* state, vector< Message >& messages);
void handle_quit(int comm_fd, int* state, char* user, vector< Message >& messages, bool* quit);
void write_response(int comm_fd, const char* response);
void copy_command(char* dest, char* src);
void read_file(vector< Message >& messages, char* src);
void list_all(int comm_fd, vector< Message >& messages);
void list_one(int comm_fd, char* command, vector< Message >& messages);
void uidl_all(int comm_fd, vector< Message >& messages);
void uidl_one(int comm_fd, char* command, vector< Message >& messages);
void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer);
void remove_command(char* buf, char* end);


// Main function of the program. Also the dispatcher of worker threads. This function parses command line 
// arguments, set up the server, and dispatches worker threads to handle connections.
int main(int argc, char *argv[]) {
	// signal handler
	signal(SIGINT, signal_handler);

	int option = 0;
	// port defaults to 11000 if no arguments given
	unsigned short port = 11000;

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
			cerr << "Usage: " << argv[0] << " [-p port number] [-a] [-v] <mailbox directory>\r\n";
			exit(1);
		}
	}

	// if no mailbox directory given
	if (optind == argc) {
		cerr << "Usage: " << argv[0] << " [-p port number] [-a] [-v] <mailbox directory>\r\n";
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

		if (DEBUG) cerr << "[" << fd << "] " << NEW_CONN;

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
		while ((entry = readdir(mbdir)) != NULL) {
			if (strlen(entry->d_name) > 2) {
				string mailbox(entry->d_name);
				MAILBOXES.insert(mailbox);
			}
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
	int state = AUTHORIZATION;

	// buffers for client's command
	char buf[BUFFER_SIZE];
	char* curr = buf;
	bool quit = false;

	char user[MAILBOX_LEN];
	vector< Message > messages;

	// into one connection
	while (!quit) {
		int curr_len = strlen(buf);
		int rlen = read(comm_fd, curr, BUFFER_SIZE - curr_len);
		char* end = (char*)malloc(sizeof(char*));

		// into one command - if command contains "<CR><LF>", enter loop
		while ((end = strstr(buf, "\r\n")) != NULL) {
			// move end to the end of "<CR><LF>"
			end += 2;

			if (DEBUG) fprintf(stderr, "[%d] C: %.*s", comm_fd, (int)(end - buf), buf);

			char command[COMMAND_LEN];
			for (int i = 0; i < COMMAND_LEN; i++) {
				command[i] = buf[i];
			}

			// USER response
			if (strcasecmp(command, "user") == 0) {
				handle_user(comm_fd, &state, buf, user);
			
			// PASS response
			} else if (strcasecmp(command, "pass") == 0) {
				handle_pass(comm_fd, &state, buf, user, messages);
			
			// STAT response
			} else if (strcasecmp(command, "stat") == 0) {
				handle_stat(comm_fd, &state, messages);
			
			// LIST response
			} else if (strcasecmp(command, "list") == 0) {
				handle_list(comm_fd, &state, buf, messages);

			// UIDL response
			} else if (strcasecmp(command, "uidl") == 0) {
				handle_uidl(comm_fd, &state, buf, messages);

			// RETR response
			} else if (strcasecmp(command, "retr") == 0) {
				handle_retr(comm_fd, &state, buf, messages);

			// DELE response
			} else if (strcasecmp(command, "dele") == 0) {
				handle_dele(comm_fd, &state, buf, messages);

			// NOOP response
			} else if (strcasecmp(command, "noop") == 0) {
				handle_noop(comm_fd, &state);

			// RSET response
			} else if (strcasecmp(command, "rset") == 0) {
				handle_rset(comm_fd, &state, messages);

			// QUIT response
			} else if (strcasecmp(command, "quit") == 0) {
				handle_quit(comm_fd, &state, user, messages, &quit);

			// unknown command response
			} else {
				write_response(comm_fd, UNRECGONIZED_COMMAND);
			}

			if (quit) break;

			// clear buffer of one full command
			remove_command(buf, end);
		}

		if (quit) break;

		// reset curr to point to the end of buffer
		curr = buf;
		while (*curr != '\0') {
			curr++;
		}
		free(end);
	}

	close(comm_fd);
	if (DEBUG) cerr << "[" << comm_fd << "] " << CLOSE_CONN;
	pthread_exit(NULL);
}

// Handler for USER command. Checks whether the transaction is at the correct state and send response
// accordingly. Checks if user exists.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// user:		buffer to record user name
void handle_user(int comm_fd, int* state, char* buffer, char* user) {
	if (*state != AUTHORIZATION || strlen(user) != 0) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char mailbox[MAILBOX_LEN];
		copy_command(mailbox, buffer);
		string mbox(mailbox);
		mbox += ".mbox";

		if (MAILBOXES.find(mbox) != MAILBOXES.end()) {
			write_response(comm_fd, USER_EXISTS);
			strcpy(user, mailbox);
		} else {
			write_response(comm_fd, NO_USER);
		}
	}
}

// Handler for PASS command. Checks whether the transaction is at the correct state and send response
// accordingly. If the password is correct, enters transaction state and reads mails from mailbox; if not, 
// user name is reset to  empty.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// user:		user name
// messages:	container to keep track of messages
void handle_pass(int comm_fd, int* state, char* buffer, char* user, vector< Message >& messages) {
	if (*state != AUTHORIZATION || strlen(user) == 0) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char password[MAILBOX_LEN];
		copy_command(password, buffer);

		if (strcmp(password, "cis505") == 0) {
			*state = TRANSACTION;
			read_file(messages, user);
			write_response(comm_fd, VALID_PASSWORD);
		} else {
			memset(user, 0, strlen(user));
			write_response(comm_fd, INVALID_PASSWORD);
		}
	}
}

// Handler for STAT command. Checks whether the transaction is at the correct state and send response
// accordingly. Displays the number and total size of messages.
// comm_fd: 	client's socket
// state: 		current transaction state
// messages:	messages in user's mailbox
void handle_stat(int comm_fd, int* state, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		string ok = "+OK ";
		int count = 0;
		int chars = 0;

		for (int i = 0; i < messages.size(); i++) {
			if (!messages[i].deleted) {
				count++;
				chars += messages[i].content.length();
			}	
		}

		ok = ok + to_string(count) + " " + to_string(chars) + "\r\n";
		write_response(comm_fd, ok.c_str());
	}
}

// Handler for LIST command. Checks whether the transaction is at the correct state and send response
// accordingly. Checks if LIST is followed by an argument and calls a corresponding helper function.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// messages:	messages in user's mailbox
void handle_list(int comm_fd, int* state, char* buffer, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char command[COMMAND_LEN];
		copy_command(command, buffer);
		if (strlen(command) == 0) {
			list_all(comm_fd, messages);
		} else {
			list_one(comm_fd, command, messages);
		}
	}
}

// Handler for UIDL command. Checks whether the transaction is at the correct state and send response
// accordingly. Checks if UIDL is followed by an argument and calls a corresponding helper function.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// messages:	messages in user's mailbox
void handle_uidl(int comm_fd, int* state, char* buffer, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char command[COMMAND_LEN];
		copy_command(command, buffer);
		if (strlen(command) == 0) {
			uidl_all(comm_fd, messages);
		} else {
			uidl_one(comm_fd, command, messages);
		}
	}
}

// Handler for RETR command. Checks whether the transaction is at the correct state and send response
// accordingly. Prints a message to client's console.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// messages:	messages in user's mailbox
void handle_retr(int comm_fd, int* state, char* buffer, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char command[COMMAND_LEN];
		copy_command(command, buffer);

		if (strlen(command) == 0) {
			write_response(comm_fd, UNRECGONIZED_COMMAND);
		} else {
			int index = atoi(command);

			if (index < 1 || index > messages.size() || messages[index - 1].deleted) {
				write_response(comm_fd, NO_MESSAGE);
			} else {
				string message = messages[index - 1].content;
				string res = "+OK " + to_string(message.length()) + " octets\r\n";
				write_response(comm_fd, res.c_str());

				int i = 0;
				while (i < message.length()) {
					int end = message.find('\n', i);
					string line = message.substr(i, end + 1 - i);
					write_response(comm_fd, line.c_str());
					i = end + 1;
				}

				write_response(comm_fd, ".\r\n");
			}
		}
	}
}

// Handler for DELE command. Checks whether the transaction is at the correct state and send response
// accordingly. Mark a message as deleted.
// comm_fd: 	client's socket
// state: 		current transaction state
// buffer:		master buffer for client's command
// messages:	messages in user's mailbox
void handle_dele(int comm_fd, int* state, char* buffer, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		char command[COMMAND_LEN];
		copy_command(command, buffer);

		if (strlen(command) == 0) {
			write_response(comm_fd, UNRECGONIZED_COMMAND);
		} else {
			int index = atoi(command);

			if (index < 1 || index > messages.size() || messages[index - 1].deleted) {
				write_response(comm_fd, NO_MESSAGE);
			} else {
				messages[index - 1].deleted = true;
				write_response(comm_fd, DELETED);
			}
		}
	}
}

// Handler for NOOP command. Checks whether the transaction is at the correct state and send response
// accordingly.
// comm_fd: 	client's socket
// state:		current transaction state
void handle_noop(int comm_fd, int* state) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		write_response(comm_fd, "+OK\r\n");
	}
}

// Handler for RSET command. Checks whether the transaction is at the correct state and send response
// accordingly. Undo all DELE operations.
// comm_fd: 	client's socket
// state: 		current transaction state
// messages:	messages in user's mailbox
void handle_rset(int comm_fd, int* state, vector< Message >& messages) {
	if (*state != TRANSACTION) {
		write_response(comm_fd, BAD_SEQUENCE);
	} else {
		for (int i = 0; i < messages.size(); i++) {
			if (messages[i].deleted) {
				messages[i].deleted = false;
			}
		}
		write_response(comm_fd, RESET);
	}
}

// Handler for QUIT command. Sets quit flag to true. Enters update state and deletes the messages marked as
// deleted.
// comm_fd: 	client's socket
// state: 		current transaction state
// user:		user name
// messages:	messages in user's mailbox
// quit:		true if the client writes QUIT
void handle_quit(int comm_fd, int* state, char* user, vector< Message >& messages, bool* quit) {
	// if client quits in authorization state, do not delete messages
	if (*state == AUTHORIZATION) {
		*quit = true;
		write_response(comm_fd, QUIT);
	} else if (*state == TRANSACTION) {
		unordered_set< int > deletes;
		for (int i = 0; i < messages.size(); i++) {
			if (messages[i].deleted) {
				deletes.insert(i);
			}
		}

		// create a new file and replace the old one with it
		string old_file = string(PARENTDIR) + "/" + string(user) + ".mbox";
		ifstream mbox(old_file);
		string new_file = string(PARENTDIR) + "/" + string(user) + ".mbox.new";
		ofstream out(new_file);

		string line;
		string prefix = "From ";
		bool to_delete = false;
		int i = 0;
		while (getline(mbox, line)) {
			if (line.compare(0, prefix.size(), prefix) == 0) {
				if (deletes.find(i) != deletes.end()) {
					to_delete = true;
				} else {
					to_delete = false;
					line += "\r";
				}
				i++;
			}

			if (!to_delete) {
				out << line << "\n";
			}
		}
		mbox.close();
		out.close();

		if (out) {
			remove(old_file.c_str());
			rename(new_file.c_str(), old_file.c_str());
		}

		*state = UPDATE;
		*quit = true;
		write_response(comm_fd, QUIT);
	} else {
		write_response(comm_fd, BAD_SEQUENCE);
	}
}

// Writes a response to client. Prints debug info to server's console.
// comm_fd:		client's socket
// response:	response to write to client
void write_response(int comm_fd, const char* response) {
	write(comm_fd, response, strlen(response));
	if (DEBUG) {
		fprintf(stderr, "[%d] S: %s", comm_fd, response);
	}
}

// Parse recipient's email and host name from src and copy to rcpt and host.
// rcpt: 	buffer for recipient's email address
// host: 	buffer for host name
// src:		source buffer
void copy_command(char* dest, char* src) {
	int i = 0;
	int len = strlen(src);

	while (src[i] != ' ' && i < len) {
		i++;
	}

	if (i == len) {
		dest[0] = '\0';
		return;
	}

	i++;
	int j = 0;
	while (src[i] != '\r') {
		dest[j] = src[i];
		i++;
		j++;
	}
	dest[j] = '\0';
}

// Parses mails from a .mbox file.
// messages:	container for Message objects
// src:			user name of the .mbox file
void read_file(vector< Message >& messages, char* src) {
	string mailbox = string(PARENTDIR) + "/" + string(src) + ".mbox";
	ifstream mbox(mailbox);

	string message;
	string line;
	string prefix = "From ";
	while (getline(mbox, line)) {
		if (line.compare(0, prefix.size(), prefix) == 0) {
			Message m(message);
			messages.push_back(m);
			message = "";
		} else {
			message += line;
			// end a line properly
			message.pop_back();
			message += "\r\n";
		}
	}
	mbox.close();

	// erase first message, which is empty, and append the last message.
	if (messages.size() > 0) {
		messages.erase(messages.begin());
		messages.push_back(Message(message));
	}
}

// List all messages' indexes and sizes
// comm_fd:		client's socket
// messages:	messages in user's mailbox
void list_all(int comm_fd, vector< Message >& messages) {
	int count = 0;
	int chars = 0;
	vector< string > list;
	
	for (int i = 0; i < messages.size(); i++) {
		if (!messages[i].deleted) {
			count++;
			int len = messages[i].content.length();
			chars += len;
			string line = to_string(i + 1) + " " + to_string(len) + "\r\n";
			list.push_back(line);
		}
	}

	string res = "+OK " + to_string(count) + " messages (" + to_string(chars) + " octets)\r\n";
	write_response(comm_fd, res.c_str());

	for (int i = 0; i < list.size(); i++) {
		write_response(comm_fd, list[i].c_str());
	}

	write_response(comm_fd, ".\r\n");
}

// List index and size about one of the messages
// comm_fd:		client's socket
// command:		index of the message to list
// messages:	messages in user's mailbox
void list_one(int comm_fd, char* command, vector< Message >& messages) {
	int index = atoi(command);

	if (index < 1 || index > messages.size() || messages[index - 1].deleted) {
		write_response(comm_fd, NO_MESSAGE);
	} else {
		int len = messages[index - 1].content.length();
		string res = "+OK " + to_string(index) + " " + to_string(len) + "\r\n";
		write_response(comm_fd, res.c_str());
	}
}

// List all messages' unique ids.
// comm_fd:		client's socket
// messages:	messages in user's mailbox
void uidl_all(int comm_fd, vector< Message >& messages) {
	write_response(comm_fd, UIDL_ALL);

	for (int i = 0; i < messages.size(); i++) {
		if (!messages[i].deleted) {
			unsigned char* digest = (unsigned char*)malloc(sizeof(MD5_DIGEST_LENGTH));
			char* uid = (char*)malloc(sizeof(MD5_DIGEST_LENGTH * 2));

			char msg[messages[i].content.length() + 1];
			const char* message = messages[i].content.c_str();
			strcpy(msg, message);
			
			computeDigest(msg, strlen(msg), digest);
			// convert digest to hex
			for (int j = 0; j < MD5_DIGEST_LENGTH; j++) {
				sprintf(uid + j, "%02x", digest[j]);
			}

			string res = to_string(i + 1) + " " + string(uid) + "\r\n";
			write_response(comm_fd, res.c_str());

			free(digest);
			free(uid);
		}
	}

	write_response(comm_fd, ".\r\n");
}

// List the unique id of one of the messages
// comm_fd:		client's socket
// command:		index of the message to list
// messages:	messages in user's mailbox
void uidl_one(int comm_fd, char* command, vector< Message >& messages) {
	int index = atoi(command);

	if (index < 1 || index > messages.size() || messages[index - 1].deleted) {
		write_response(comm_fd, NO_MESSAGE);
	} else {
		unsigned char* digest = (unsigned char*)malloc(sizeof(MD5_DIGEST_LENGTH));
		char* uid = (char*)malloc(sizeof(MD5_DIGEST_LENGTH * 2));
		char msg[messages[index - 1].content.length() + 1];
		
		const char* message = messages[index - 1].content.c_str();
		strcpy(msg, message);
		
		computeDigest(msg, strlen(msg), digest);
		// convert digest to hex
		for (int j = 0; j < MD5_DIGEST_LENGTH; j++) {
			sprintf(uid + j, "%02x", digest[j]);
		}

		string res = "+OK " + to_string(index) + " " + string(uid) + "\r\n";
		write_response(comm_fd, res.c_str());

		free(digest);
		free(uid);
	}
}

// Compute the MD5 hash of a given string.
// data:			input string
// dataLengthBytes:	length of input string
// digestBuffer:	result buffer
void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer){
	/* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */

	MD5_CTX c;
	MD5_Init(&c);
	MD5_Update(&c, data, dataLengthBytes);
	MD5_Final(digestBuffer, &c);
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
