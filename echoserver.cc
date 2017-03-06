#include <arpa/inet.h>
#include <iostream>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>

using namespace std;

// constant strings
const char* GREETING 		= "+OK Server ready (Author: Han Zhu / zhuhan)\r\n";
const char* NEW_CONN 		= "New connection\r\n";
const char* PREFIX 			= "+OK ";
const char* GOODBYE 		= "+OK Goodbye!\r\n";
const char* UNKNOWN_COMMAND = "-ERR Unknown command\r\n";
const char* CLOSE_CONN 		= "Connection closed\r\n";
const char* SHUT_DOWN 		= "-ERR Server shutting down\r\n";

// global variables
vector< pthread_t > THREADS;
vector< int > SOCKETS;
bool DEBUG = false;

// function signatures
void signal_handler(int arg);
void* worker(void* arg);
void removeCommand(char* buf, char* end);

// Main function of the program. Also the dispatcher of worker threads. This function parses command line 
// arguments, set up the server, and dispatches worker threads to handle connections.
int main(int argc, char *argv[]) {
	// signal handler
	signal(SIGINT, signal_handler);

	int option = 0;
	// port defaults to 10000 if no arguments given
	unsigned short port = 10000;

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
			cerr << "Usage: " << argv[0] << " [-p port_number] [-a] [-v]\r\n";
			exit(1);
		}
	}

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
	cout << "\r\n";

	for (int i = 1; i < SOCKETS.size(); i++) {
		write(SOCKETS[i], SHUT_DOWN, strlen(SHUT_DOWN));
		close(SOCKETS[i]);
		pthread_kill(THREADS[i - 1], 0);
	}
}

// Worker thread that handles the connection. One thread for one client.
// arg: file descriptor of the socket the client connects to.
void* worker(void* arg) {
	int comm_fd = *(int*)arg;
	write(comm_fd, GREETING, strlen(GREETING));

	// buffer for client's command
	char buf[1000];
	char* curr = buf;
	bool is_quit = false;

	while (true) {
		int curr_len = strlen(buf);
		int rlen = read(comm_fd, curr, 1000 - curr_len);
		char* end = (char*)malloc(sizeof(char*));

		// if command contains "<CR><LF>", enter loop
		while ((end = strstr(buf, "\r\n")) != NULL) {
			// move end to the end of "<CR><LF>"
			end += 2;

			char command[5];
			for (int i = 0; i < 4; i++) {
				command[i] = buf[i];
			}

			int command_len = strlen(command);
			char* start = buf + command_len + 1;
			char message[1000];

			// ECHO response
			if (strcasecmp(command, "echo") == 0) {
				strcpy(message, PREFIX);
				int message_len = end - start;
				strncpy(&message[4], start, message_len);
				message[message_len + 4] = '\0';
				write(comm_fd, message, message_len + 4);
			// QUIT response
			} else if (strcasecmp(command, "quit") == 0) {
				strcpy(message, GOODBYE);
				write(comm_fd, message, strlen(message));
				is_quit = true;

				if (DEBUG) {
					fprintf(stderr, "[%d] C: %s\r\n", comm_fd, command);
					fprintf(stderr, "[%d] S: %s", comm_fd, message);
				}
				break;
			// unknown command response
			} else {
				strcpy(message, UNKNOWN_COMMAND);
				write(comm_fd, message, strlen(message));
			}

			if (DEBUG) {
				fprintf(stderr, "[%d] C: %.*s", comm_fd, (int)(end - buf), buf);
				fprintf(stderr, "[%d] S: %s", comm_fd, message);
			}

			// clear buffer of one full command
			removeCommand(buf, end);
		}

		if (is_quit) {
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

// After worker thread has parsed a full command, this function clears the command from buffer, and moves
// all remaining characters to the start of buffer.
void removeCommand(char* buf, char* end) {
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
