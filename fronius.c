/* FRONIUS Interface program */

#include <stdio.h>      // for FILE
#include <stdlib.h>     // for timeval
#include <string.h>     // for strlen etc
#include <time.h>       // for ctime
#include <sys/types.h>  // for fd_set
#include <netdb.h>      // for sockaddr_in 
#include <fcntl.h>      // for O_RDWR
#include <termios.h>    // for termios
#include <getopt.h>     // for getopt
#include <sys/mman.h>	// for PROT_READ
#include <errno.h>      // For ETIMEDOUT
#include "../Common/common.h"

/* Version 0.0 22/03/2007 Created by copying from Victron */
// 0.1 29/04/2007 On-site corrections - ignore Exponent = 11 during Startup phase.
// 0.1.1 10/05/2007 Minor tweaks to debugging output
// 0.1.2 25/05/2007 Correct incorrect exponent data
// 1.4 13/07/2007 Timestamp on output. Use decimal. 
// 1.5 17/02/2008 Suppress Exponent Error messages except during debug
// 1.6 01/03/2008 Produce decent Inverter Status messages
// 1.7 19/05/2008 Support for multiple instances
// 1.8 Better support for multiple instances!
// 1.9 23/07/2008 Support for multiple inverters on DATCOM
// 1.10 10/09/2008 Fixed bug where it was using DEBUGCOMMS. Also removed CRTSCTS to allow it to work on TS-SER1 + 4
// 1.11 03/10/2008 Fixed Core dump at startup when debugging is off.
// 1.12 18/11/2008 Add locking of serial device and startup when there are no active inverters.
// 1.13 21/01/2009 Enable ErrorI Forwarding and Error Response
// 1.14 26/02/2009 Activate Error: start with 1 and go up to 31 before giving up.  NOT TESTED ON SITE
// 1.15 08/03/2009 If a command times out, go onto next one instead of repeating it.
// 1.16 13/04/2009 Activate Error: if n=1; assume singleton else use date.
// 1.17 20/04/2009 Flash the Red LED as data comes in
// 1.18 09/08/2009 Support for hostname:portnum syntax for specifying device path. See remoteserial for code.
// 1.19 05/10/2009 1.18 broke support for serial port. Spurious ProcessComm messages about 0 bytes/Success
//	Also suppress unlikely data values
// 1.20 20/10/2009 Problem if IGNR is 0 - breaks server comms
// 1.21 02/12/2009 Suppress non-header byte message storms
// 1.22 19/03/2010 Added IG+ inverter types
// 1.23 28/03/2010 Increased MAXINVERTER to 13 from 8.
// 1.24 31/03/2010 Introduced count of unlikely values and ignore IGNR of 34 
// 1.25 21/04/2010 Use -O for old format; -N for new format. Current default will be -O
// 1.26 10/05/2010 Bugfix - when inverters = MAXINVERTERS, bounds violation on count[] and responseVAL (out by 1 error)
// 1.27 19/08/2010 Bugfix - core dumped when initial serial device not found
// 1.28 28/09/2010 Report sudden AC voltage variations
// 1.29 10/11/2010 More information in WARNING ACV messages
// 1.30 30/12/2010 Support TL inverters on RS845/422. Default is now DataDictionary.
// 1.31 18/01/2011 Bugfix - more resilient to comms errors (short packets) for TL on RS422
// 1.32 28/01/2011 Bugfix - suppress invalid command messages; clarify overvoltage message
// 1.33 23/02/2011 Increase timeout from 10mSec to 100mSec for serial extender at Keninston
// 1.34 08/03/2011 Permit 3-phase inverters where VAC ~ 417.
// 1.35 28/03/2011 Improve handling of short packets that lead to large number of checksum errors.
// 1.36 21/04/2011 Bugfix -n didn't work due to improper parameters to OpenSockets (When was this introduced?)
// 1.37 02/02/2012 Bugfix - used Energy for Year not Energy Total.
// --
// 2.0 30/05/2010 Uplift to 2.0

#define REVISION "$Revision: 1.37 $"
static char* id="@(#)$Id: fronius.c,v 1.37 2012/01/02 19:08:36 martin Exp $";

#define PORTNO 10010
#define PROGNAME "Fronius"
#define LOGON "fronius"
const char progname[] = "fronius";
#define LOGFILE "/tmp/fronius%d.log"
#define SERIALNAME "/dev/ttyAM0"        /* although it MUST be supplied on command line */
#define HOSTNAME "localhost"

// Severity levels.  FATAL terminates program
// #define INFO    0
// #define WARN    1
// #define ERROR   2
// #define FATAL   3
// Socket retry params
#define NUMRETRIES 3
int numretries = NUMRETRIES;
#define RETRYDELAY      1000000 /* microseconds  = 1 sec */
int retrydelay = RETRYDELAY;
// Serial retry params
#define SERIALNUMRETRIES 10
#define SERIALRETRYDELAY 1000000 /*microseconds = 1 sec */
#define WAITTIME 2      /*seconds*/
// Set to if(0) to disable debugging
// #define DEBUG if(debug)
// #define DEBUG2 if(debug > 1)
// #define DEBUGFP stderr   /* set to stderr or logfp as required */

// This allows use of stdio instead of the serial device, and you can type in the hex value
// #define DEBUGCOMMS

#define VARSTART 0x10 /* First value to collect */
#define VAREND  0x18 /* Last value to collect */
#define MAXINVERTERS 12

enum Format {old = 0, dataDictionary} dataFormat = dataDictionary;

// Commands
enum Commands {GETVERSION = 1, GETDEVICETYPE, GETDATETIME, GETACTIVEINVERTERS, 
	SETERRORSENDING = 7, SETERRORFORWARDING = 13, PROTOCOLERROR, ERRORSTATE};

// System type
enum {unset = 0, datalogger, ifceasy, rs485, lastType} systemType = unset;
char *systemStr[] = {"unset", "Datalogger", "IFC Easy", "RS422", 0};

int numInverters = 0;
int currentInverter = 0;
unsigned char inverter[MAXINVERTERS] = {1};

float responseVal[MAXINVERTERS][VAREND - VARSTART + 1];	// 9 values per inverter
char count[MAXINVERTERS][VAREND - VARSTART + 1];	// Count for unlikely values
int exponent[VAREND - VARSTART + 1] = {0, 3, 3, 3, -2, 0, -2, -2, 0};
int exponenterror = 0;		// In exponent error mode?

enum CommandType { INVALID, GetVersion = 1, GetDevType, GetActiveInverters = 4, 
	GetVals, ActivateError};
char *CommandName[] = {"INVALID", "GetVersion", "GetDevType", "INVAL 3", "GetActiveInverters", "GetVals", "ActivateErrorForwarding"};
/* To handle initiating ActivateErrorState. IF we are easInit, we are trying numbers one at a time until it succeeds, as part of the 
start up sequence.  Once we have succeeded or failed, we go into easComplete and any ActivateErrorForwarding commands
are being entered interactively */
enum  {easInit, easComplete} errorActivateState; 

// Default command to repeat
#define CMD GetVals

struct {
        int commandIndex;       // current index into GetVals
        int commandLimit;       // where to stop
        int commandComplete;            // TRUE if this command response has been received so another can be sent
                /* only indicates a complete packet from Fronius, it might be unsolicited */
        int awaitReply;         // waiting for reply before sending next command
                /* If True: we have not seen the correct response yet.  Do not set if the command does not
                have a response, for example Switch.
                If False: we can send another command */
        int currentSequence;            // enum of current sequence
        int nextSequence;                       // enum of next sequence
        int sequenceComplete;           // TRUE when we can start the next sequence 
        int     varID;                  // Which RAMVar or setting to read/write
        float varValue;         // Value read or to be written
		int responseLength;		// Length of incoming packet
} staticInfo;

#define QUEUESIZE 10
struct queue {
        int top, bottom;
        enum CommandType type[QUEUESIZE];
        int  param[QUEUESIZE]; 
        float val[QUEUESIZE];
} queue; 

/* Command line params: 
1 - device name
2 - controller num.

options: device timeout. Default to 60 seconds
*/

#ifndef linux
extern
#endif
int errno; 

int inverterStatus, prevInverterStatus = -1;	// bitmask of active inverters.
int servers = 1;

// Procedures used
// int openSerial(const char * name, int baud, int parity, int databits, int stopbits);  // return fd
// void closeSerial(int fd);  // restore terminal settings
// void sockSend(const int fd, const char * msg);        // send a string
void processComm(int commfd);           // get one byte from serial port
int processSocket(void);                        // process server message
void processPacket(unsigned char * buf);                // validate complete packet
// void logmsg(int severity, char *msg);   // Log a message to server and file
int sendSerial(int fd, unsigned char data);     // Send a byte
int     sendCommand(int fd, unsigned char dev, unsigned char num, unsigned char cmd); // Send a command
int     sendCommand2(int fd, unsigned char dev, unsigned char num, 
	unsigned char cmd, unsigned char param1, unsigned char param2); // Send a command with 2 params
int sendCommandN(int fd, unsigned char dev, unsigned char num, unsigned char cmd, int howmany, unsigned char * params);
void usage(void);                                               // Standard usage message
char * deviceType(int n);
char * getversion(void);			// Convert $REVISION$ macro
char * getTime(void);			// formatted timestamp
float sanitycheck(float value, int index, float prev, char * count);	// Check value against previous
int getbuf(int fd, int max, int mSec);
void dumpbuf();
char * protocolError(int n);		// decode a protocol error return
char * statusText(int n);			// decode a Status value

// Globals
FILE * logfp = NULL;
int sockfd[MAXINVERTERS];
int debug = 0;
int noserver = 0;               // Set to 1 to prevent socket connection.
int BAUD = B19200;				// It's normally a #define

#define BUFSIZE (10 + 12 + MAXINVERTERS)      /* A packet is up to 12 bytes except GetActiveInverters */
// Common Serial Framework
struct data {	// The serial buffer
	int count;
	unsigned char buf[BUFSIZE];
	int status;
} data;
unsigned char serialbuf[BUFSIZE];       // data accumulates in this global
int serbufindex = 0;
int controllernum = 0;  // only used for logon message
char buffer[256];
char * serialName = SERIALNAME;
unsigned int errorParam1 = 2, errorParam2 = 0x55;	// This is suitable for Interface Card Easy

/********/
/* MAIN */
/********/
int main(int argc, char *argv[])
// arg1: serial device file
// arg2: optional timeout in seconds, default 60
// arg3: optional 'nolog' to carry on when filesystem full
{
    int commfd = 0;
	int nolog = 0;
	int online = 1;                 // assume it's online to start with.
	int option;                             // command line processing
	int fake = 0;                   // send fake data
	time_t commandSent;
	int run = 1;
	fd_set readfd; 
	int numfds;
	struct timeval timeout;
	int tmout = 60;
	int logerror = 0;
	int i;
	int waittime = WAITTIME;
	
	// Turn off Red LED
	blinkLED(0, REDLED);
	
	bzero(responseVal, sizeof(responseVal));
	bzero(count, sizeof(count));
	
	// Command line arguments
	
	opterr = 0;
	while ((option = getopt(argc, argv, "dt:n:slfV0123ZON")) != -1) {
		switch (option) {
			case '0': BAUD = B2400; break;
			case '1': BAUD = B4800; break;
			case '2': BAUD = B9600; break;
			case '3': BAUD = B19200; break;
			case 's': noserver = 1; break;
			case 'l': nolog = 1; break;
			case '?': usage(); exit(1);
			case 't': tmout = atoi(optarg); break;
			case 'd': debug++; break;
			case 'f': fake = 1; break;
			case 'n': servers = atoi(optarg); break;
			case 'w': waittime = atoi(optarg); break;
			case 'O': dataFormat = old; break;
			case 'N': dataFormat = dataDictionary; break;
			case 'V': printf("Version: %s %s\n", getversion(), id); exit(0);
			case 'Z': decode("(b+#Gjv~z`mcx-@ndd`rxbwcl9Vox=,/\x10\x17\x0e\x11\x14\x15\x11\x0b\x1a" 
							 "\x19\x1a\x13\x0cx@NEEZ\\F\\ER\\\x19YTLDWQ'a-1d()#!/#(-9' >q\"!;=?51-??r"); exit(0);
		}
	}
	
#ifdef		DEBUGCOMMS
#undef DEBUGFP
#define DEBUGFP stderr
#endif
	if (optind < argc) serialName = argv[optind];
	optind++;
	if (optind < argc) controllernum = atoi(argv[optind]);
	sprintf(buffer, LOGFILE, controllernum);

	if (!nolog) if ((logfp = fopen(buffer, "a")) == NULL) logerror = errno;
	
	// There is no point in logging the failure to open the logfile
	// to the logfile, and the socket is not yet open.
	
	sprintf(buffer, "STARTED %s on %s as %d timeout %d %s %s", argv[0], serialName, 
			controllernum, tmout, nolog ? "nolog" : "", fake ? "(fake)" : "");
	logmsg(WARN, buffer);
	
	// initialise data
	
	queue.top = queue.bottom = 0;
	
	// Set up socket 
	if (servers) {
		if (dataFormat == old)
			openSockets(0, servers, LOGON, REVISION, "", 0);
		else
			openSockets(0, servers, "inverter", REVISION, PROGNAME, 0);
	}
	else
		exit(0);
	
	// Set up activateErrorForeading if numserver > 1 to be the day of month.
	if (servers > 1 ) {
		struct tm * tmp;
		time_t t;
		time(&t);
		tmp = localtime(&t);
		DEBUG fprintf(DEBUGFP, "Using date as %d in ActivateError .. ", tmp->tm_mday);
		errorParam1 = tmp->tm_mday;
	}
	
	// Open serial port
#ifdef DEBUGCOMMS
	commfd = 0;
#else
	if (!fake) 
        if ((commfd = openSerial(serialName, BAUD, 0, CS8, 1)) < 0) {
			sprintf(buffer, "FATAL " PROGNAME " %d Failed to open %s at %d: %s", controllernum, serialName, BAUD, strerror(errno));
			logmsg(FATAL, buffer);
        }
	
#endif

	if (flock(commfd, LOCK_EX | LOCK_NB) == -1) {
		sprintf(buffer, "FATAL " PROGNAME " is already running, cannot start another one on %s", serialName);
		logmsg(FATAL, buffer);
	}


	// If we failed to open the logfile and were NOT called with nolog, warn server
	if (logfp == NULL && nolog == 0) {
		sprintf(buffer, "event WARN " PROGNAME " %d could not open logfile %s: %s", controllernum, LOGFILE, strerror(logerror));
		sockSend(sockfd[0], buffer);
	}
	
	numfds = (sockfd[0] > commfd ? sockfd[0] : commfd) + servers + 1;              // nfds parameter to select. One more than highest descriptor
	DEBUG2 fprintf(DEBUGFP, "Numfds = %d Commfd = %d max Sockfd = %d", numfds, commfd, sockfd[0]);
	// Main Loop
	FD_ZERO(&readfd); 
	staticInfo.commandIndex = 0;
	staticInfo.commandComplete = 1;
	staticInfo.currentSequence = GetVersion;   //Start with a GetVersion to see if it's alive
	staticInfo.nextSequence = ActivateError;	// Was GetActiveInverters;
	staticInfo.awaitReply = 0;
	errorActivateState = easInit;		// This will initially send 02 from errorParam1, for Interface Card Easy.
	commandSent = time(NULL);
	data.count = 0;
	
	//      sendInit(commfd);
	while(run) {
		
		// Main loop
		
		if (staticInfo.commandComplete) {               // prepare to send next command 
			DEBUG fprintf(DEBUGFP, "Command complete - pausing before next one ");
			sleep(waittime);         // 1 or 2 seconds
			if (staticInfo.sequenceComplete) {  // Set up for next sequence
				staticInfo.sequenceComplete = 0;
				if (queue.top != queue.bottom) {        // get command from queue
					queue.bottom++;
					if (queue.bottom == QUEUESIZE) queue.bottom = 0;
					DEBUG2 fprintf(DEBUGFP, "Queue len %d Getting command from index %d: %s\n", 
								  queue.top - queue.bottom + 1, queue.bottom,
								  CommandName[queue.bottom]);
					staticInfo.currentSequence = queue.type[queue.bottom];
				}
				else {          // in idle mode alternate between GetVals and GetActive Inverters.
								// unless numinverters is zero, in which case keep querying until we get
								// some active inverters.
					staticInfo.currentSequence = staticInfo.nextSequence;
					if (staticInfo.currentSequence == GetActiveInverters && numInverters > 0)
						staticInfo.nextSequence = GetVals;
					else
						staticInfo.nextSequence = GetActiveInverters;
				}
				DEBUG2 fprintf(DEBUGFP, "\nNew Sequence %s then %s ", CommandName[staticInfo.currentSequence], 
							  CommandName[staticInfo.nextSequence]);
				
				if (staticInfo.currentSequence == GetVals) {
					staticInfo.commandLimit = VAREND;
					staticInfo.commandIndex = VARSTART;
				}
			}
			commandSent = time(NULL);
			switch(staticInfo.currentSequence) {
				case GetVersion:
					DEBUG fprintf(DEBUGFP, "\nCMD: GetVersion ");
					sendCommand(commfd, 0, 0, GETVERSION);
					break;
				case GetDevType:
					DEBUG fprintf(DEBUGFP, "\nCMD: GetDevType of %d ", inverter[currentInverter]);
					sendCommand(commfd, 1, inverter[currentInverter], GETDEVICETYPE);	break;
				case GetActiveInverters:
					DEBUG fprintf(DEBUGFP, "\nCMD: ActiveInverters ");
					sendCommand(commfd, 0, 0, GETACTIVEINVERTERS);	break;
				case GetVals:
					DEBUG fprintf(DEBUGFP, "\nCMD: GetVal %d for Inv %d ", staticInfo.commandIndex, inverter[currentInverter]);
					sendCommand(commfd, 1, inverter[currentInverter], staticInfo.commandIndex);
					break;
				case ActivateError:
					if (systemType == rs485) {	// Use ErrorSending
						unsigned char invs[MAXINVERTERS];
						int i;
						invs[0] = 0x55;		// Magic value to validate ErrorSending
						for (i  = 1; i <= servers; i++)
							invs[i] = i;
						DEBUG fprintf(DEBUGFP, "\nCMD: ActivateErrorSending ");
						sendCommandN(commfd, 0, 0, SETERRORSENDING, i, invs);
					} else {
						// Should change this to use SendCommandN, and to use systemType to decide whether to send Date or 2.
						DEBUG fprintf(DEBUGFP, "\nCMD: ActivateError %02x %02x ", errorParam1, errorParam2);
						sendCommand2(commfd, 0, 0, SETERRORFORWARDING, errorParam1, errorParam2);
					}
					break;
				default:
					logmsg(ERROR, "ERROR not coded for this");
			}
			// Set awaitReply flag
			staticInfo.awaitReply = 1;
		}
		timeout.tv_sec = tmout;
		timeout.tv_usec = 0;
		data.count = 0;
		bzero(data.buf, sizeof(data.buf));
		for (i = 0; i < servers; i++)
			FD_SET(sockfd[i], &readfd);
		if (!fake)      FD_SET(commfd, &readfd);
		if (select(numfds, &readfd, NULL, NULL, &timeout) == 0) {       // select timed out. Bad news 
			// Set CommandComplete so it moves onto next command in sequence
			staticInfo.commandComplete = 1;
			staticInfo.sequenceComplete = 1;
			if (fake) {
				if (dataFormat == old)
					sockSend(sockfd[0], "data 9 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0");
				else
					sockSend(sockfd[0], "inverter watts:120 kwh:137000 iac:0.49 vac:245.0 hz:49.990 idc:0.60 vdc:239.0");
			} else
				if (online) {
					sprintf(buffer, "WARN " PROGNAME " %d No data for last period", controllernum);
					logmsg(WARN, buffer);
					online = 0;     // prevent recurring messages
				}
			
			continue;
		}
		// Want to loop here consuming anything from the Fronius
		if (!fake)
			while (FD_ISSET(commfd, &readfd)) {
				int num;
				blinkLED(1, REDLED);
				online = 1;     // back on line
				num = getbuf(commfd, 50, 100);		// V1.33 - change from 10 to 100msEc due to using serial externder
				DEBUG fprintf(stderr,"Getbuf: %d (%d)\n", num, data.count);
				DEBUG dumpbuf();
				// processComm(commfd);
				processPacket(data.buf);
				FD_SET(commfd, &readfd);
				timeout.tv_sec = 0;
				timeout.tv_usec = 10000;        // see if another character is on its way.  (10mSec)
				select(commfd+1, &readfd, NULL, NULL, &timeout);
				// DEBUG fprintf(DEBUGFP, "%s", FD_ISSET(commfd, &readfd) ? "Y" : "N");
			}
			blinkLED(0, REDLED);
		
		if ((noserver == 0) && FD_ISSET(sockfd[0], &readfd)) {
			DEBUG fprintf(DEBUGFP, "\nCalling ProcessSocket (fd %d)\n", sockfd[0]);
			run = processSocket();  // the server may request a shutdown so set run to 0
		}
		// DEBUG fprintf(DEBUGFP, "AwaitReply: %d ", staticInfo.awaitReply);
		if (staticInfo.awaitReply == 1 && time(NULL) < commandSent + WAITTIME) { //waiting .. 
			DEBUG2 fprintf(DEBUGFP, "looping .. ");
			continue;
		}
		if (staticInfo.awaitReply == 1 && time(NULL) > commandSent + WAITTIME) { //timeout
			DEBUG fprintf(DEBUGFP, "\n*** Timeout %d ***\n", WAITTIME);
		}
		if (fake) continue;
	}
	sprintf(buffer,"INFO " PROGNAME " %d Shutdown requested", controllernum);
	logmsg(INFO, buffer);
	for (i = 0; i < servers; i++)
		close(sockfd[i]);
	closeSerial(commfd);
	return 0;
}

/*********/
/* USAGE */
/*********/
void usage(void) {
        printf("Usage: fronius [-t timeout] [-l] [-s] [-d] [-f] [-V] [O|N] [-01234] [-n XXX] [-w n] /dev/ttyname controllernum \n");
        printf("-l: no log  -s: no server  -d: debug on -f: fake data -V version -n number of slaves (0 for a slave) -w wait time\n");
		printf("Speed: 0=2400, 1=4800, 2=9600, 3=14400; 4=19200 format: O[ld] N[ew]\n");
        return;
}

/**************/
/* SENDSERIAL */
/**************/
int sendSerial(int fd, unsigned char data) {
	// Send a single byte.  Return 1 for a logged failure
	int retries = SERIALNUMRETRIES;
	int written;
	int newfd;
#ifdef DEBUGCOMMS
	fprintf(DEBUGFP, "Comm 0x%02x(%d) ", data, data);
	return 0;
#endif
	
	DEBUG2 fprintf(DEBUGFP, "%02x ", data);
	while ((written = write(fd, &data, 1)) < 1) {
        fprintf(DEBUGFP, "Serial wrote %d bytes errno = %d", written, errno);
		sprintf(buffer, "WARN " PROGNAME " %d SendSerial: Failed to write data: %s", controllernum, strerror(errno));
		logmsg(INFO, buffer);
		close(fd);
		newfd = openSerial(serialName, BAUD, 0, CS8, 1);
		if (newfd < 0) {
			sprintf(buffer, "WARN " PROGNAME " %d SendSerial: Error reopening serial/port: %s ", controllernum, strerror(errno));
			logmsg(WARN, buffer);
		}
		if (newfd != fd) {
			sprintf(buffer, "WARN " PROGNAME " %d SendSerial: Problem reopening socket - was %d now %d", controllernum, fd, newfd);
			logmsg(WARN, buffer);
			return 1;
		}
		if (--retries == 0) {
			sprintf(buffer, "WARN " PROGNAME " %d SendSerial: too many retries", controllernum);
			logmsg(WARN, buffer);
			return 1;
		}
		DEBUG fprintf(DEBUGFP, "SendSerial retry pausing %d ... ", SERIALRETRYDELAY);
		usleep(SERIALRETRYDELAY);
	}
	return 0;       // ok
}

/***************/
/* SENDCOMMAND */
/***************/
int sendCommand(int fd, unsigned char dev, unsigned char num, unsigned char cmd) {
	// As before, return 1 for a logged failure, otherwise 0
	unsigned char   checksum;
	
	DEBUG2 fprintf(DEBUGFP, "\n%s SendCommand: dev/opt %d num %d (0x%02x) cmd %d (0x%02x) \n", getTime(), dev, num, num, cmd, cmd);
#ifndef DEBUGCOMMS
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x00)) return 1;	// Length : always 00 for sent commands
	if (sendSerial(fd, dev)) return 1;
	if (sendSerial(fd, num)) return 1;
	if (sendSerial(fd, cmd)) return 1;
	checksum = dev + num + cmd;
	if (sendSerial(fd, checksum & 0xFF)) return 1;
#endif
	return 0;
}

/****************/
/* SENDCOMMAND2 */
/****************/
// Send command with 2 parameters
int sendCommand2(int fd, unsigned char dev, unsigned char num, unsigned char cmd, unsigned char param1, unsigned char param2) {
	// As before, return 1 for a logged failure, otherwise 0
	unsigned char   checksum;
	
	DEBUG2 fprintf(DEBUGFP, "\n%s SendCommand2: dev/opt %d num %d (0x%02x) cmd %d (0x%02x) p1 %d (0x%02x) p2 %d (0x%02x) \n",
		getTime(), dev, num, num, cmd, cmd, param1, param1, param2, param2);
#ifndef DEBUGCOMMS
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x02)) return 1;	// Length : 02 for ActivateErrorForwarding
	if (sendSerial(fd, dev)) return 1;
	if (sendSerial(fd, num)) return 1;
	if (sendSerial(fd, cmd)) return 1;
	if (sendSerial(fd, param1)) return 1;
	if (sendSerial(fd, param2)) return 1;
	checksum = 2 + dev + num + cmd + param1 + param2;
	if (sendSerial(fd, checksum & 0xFF)) return 1;
#endif
	return 0;
}

/*****************/
/* SENDCOMMAND N */
/*****************/
// Send command with N parameters
int sendCommandN(int fd, unsigned char dev, unsigned char num, unsigned char cmd, int howmany, unsigned char * params) {
	// As before, return 1 for a logged failure, otherwise 0
	unsigned char   checksum;
	int i;
	
	DEBUG2 fprintf(DEBUGFP, "\n%s SendCommandN: dev/opt %d num %d (0x%02x) cmd %d (0x%02x) N=%d \n",
				   getTime(), dev, num, num, cmd, cmd, howmany);
#ifndef DEBUGCOMMS
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, 0x80)) return 1;
	if (sendSerial(fd, howmany)) return 1;	// Length : 02 for ActivateErrorForwarding
	if (sendSerial(fd, dev)) return 1;
	if (sendSerial(fd, num)) return 1;
	if (sendSerial(fd, cmd)) return 1;
	checksum = howmany + dev + num + cmd;
	for (i = 0; i < howmany; i++) {
		sendSerial(fd, params[i]);
		checksum += params[i];
	}
	if (sendSerial(fd, checksum & 0xFF)) return 1;
#endif
	return 0;
}

/***************/
/* PROCESSCOMM */
/***************/
void processComm(int commfd) {
	// Deal with just one byte.
	// It's easier to do it this way than read a lot then scan for the newline.
	// When we have all the bytes in the buffer, process the packet
	staticInfo.commandComplete = 0; // Always set this to FALSE on entry
	unsigned char thischar;
	int numread;
	static int commserr = 0;
	
	if (serbufindex + 1 == BUFSIZE) {
		sprintf(buffer, "WARN " PROGNAME " %d buffer overflow - discarding packet", controllernum);
		logmsg(WARN, buffer);
		serbufindex = 0;
		return;
	}
	
#ifdef DEBUGCOMMS
	{  int val;
//		char buf[20];
		numread = scanf("%x", &val);
//        gets(buf);
//        val = strtol(buf, NULL, 16);
        fprintf(DEBUGFP, " Got %02x ", val);
        serialbuf[serbufindex] = val;
	}
#else
tryagain:
	 numread = read(commfd, &serialbuf[serbufindex], 1);       // must be able to read at least one byte otherwise the 
															// fd would not be readable.
	if (numread == 0) {
		sprintf(buffer, "WARN " PROGNAME " %d read 0 bytes", controllernum);
		logmsg(WARN, buffer);
		sleep(1);
		return;
	}
	DEBUG2 fprintf(stderr, "<%02x ", serialbuf[serbufindex]);
	if (numread < 1) {
		int newfd;
		sprintf(buffer, "WARN " PROGNAME " %d ProcessComm: error reading from %s: %s. Reopening.", controllernum, 
			serialName, strerror(errno));
		logmsg(WARN, buffer);
		
		close(commfd);
		newfd = openSerial(serialName, BAUD, 0, CS8, 1);
		if (newfd < 0) perror("Reopening serial/port (FROM) ");
		if (newfd != commfd) {
			sprintf(buffer, "WARN " PROGNAME " %d Problem reopening socket - was %d now %d\n", controllernum, commfd, newfd);
			return;
		}
		sleep(1);
		goto tryagain;
	}
	
#endif
	 thischar = serialbuf[serbufindex];
	switch(serbufindex) {           // perform specific checks on each byte
		case 0:
		case 1:
		case 2:              
			if (thischar != 0x80) {
				if (!commserr) {
					sprintf(buffer, "WARN " PROGNAME " %d failed to read header byte %d as 0x80 - got 0x%02x", controllernum, serbufindex, thischar);
					logmsg(WARN, buffer);
					commserr = 1;
				}
				serbufindex = 0;
				commserr ++;
				if (commserr % 100 == 0) {
					sprintf(buffer, "WARN " PROGNAME " %d - %d non-header bytes", controllernum, commserr);
					logmsg(WARN, buffer);
				}
			}
			else {
				if (commserr) {
					sprintf(buffer, "INFO " PROGNAME " %d exiting comms error mode after %d non-header bytes", controllernum, commserr);
					logmsg(INFO, buffer);
					commserr = 0;
				}
				serbufindex++;
			}
			break;
			case 3:         // length byte.  Increase packet length to 10.
			staticInfo.responseLength = thischar;	// and fall through.
			if (thischar > MAXINVERTERS) {
				sprintf(buffer, "WARN " PROGNAME " %d got length as %d (Max is %d) - discarding packet", controllernum, thischar, MAXINVERTERS);
				logmsg(WARN, buffer);
				serbufindex = 0;
				break;
			} // Note deliberate fallthrough
			default: 
			serbufindex++; break;
	};
	 
	 if (serbufindex == staticInfo.responseLength + 8) {	// end of packet
		 int checksum = 0;
		 int i;
		 for (i = 3; i < staticInfo.responseLength + 7; i++)
			 checksum += serialbuf[i];
		 if ((checksum & 0xFF) != serialbuf[serbufindex -1]) {
			 sprintf(buffer, "WARN " PROGNAME " %d Checksum fails got %02x instead of %02x", controllernum, serialbuf[serbufindex -1], checksum);
			 logmsg(WARN, buffer);
			 serbufindex = 0;
			 return;
		 }
		 staticInfo.commandComplete = 1; // signal we have a complete packet
		 processPacket(serialbuf);
		 serbufindex = 0;
	 }
	 DEBUG2 fprintf(stderr, "Leaving processComm\n");
};

float tentothe(int n) {	// lookup function for 10^integer power within range -3 to +10
static float a[14] = {0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0,
1000000000.0, 10000000.00};
	DEBUG if (n > 10) fprintf(DEBUGFP, "Exponent Overflow %d ", n);
	DEBUG if (n < -3) fprintf(DEBUGFP, "Exponent Underflow %d ", n);
 
	if (n > 10 || n < -3) return 0;
	return a[n+3];
}

/*****************/
/* PROCESSPACKET */
/*****************/
void processPacket(unsigned char * msg) {
	// Process a packet from the Fronius
	// 1.20: validate header bytes and checksum
	
	char buffer[200];
	static int shortpacket = 0;
	int val = msg[8] + msg[7] * 256;		// All quantities are unsigned in magnitude
	int exp = (signed char) msg[9];	// hope the unsigned to signed conversion works
	int index = msg[6];
	int len = msg[3];
	float value = 0.0;
	static int have_warned = 0;		// For inverter 0 error
	static int commserr = 0;
	int i;
	DEBUG2 fprintf(DEBUGFP, "Process packet length %d ", msg[3]);

	// Validate packet
	if (data.count < msg[3] + 8) {
		shortpacket ++;
		DEBUG fprintf(stderr, "Dropping short (%d) packet\n", data.count);
		if (!shortpacket % 100) {
			sprintf(buffer, "INFO " PROGNAME " %d %d short packets dropped", controllernum, shortpacket);
			logmsg(INFO, buffer);
		}
		return;
	}
	
	for (i = 0; i < 3; i++) 
	if (msg[i] != 0x80) {
			if (!commserr) {
				sprintf(buffer, "WARN " PROGNAME " %d failed to read header byte %d as 0x80 - got 0x%02x", controllernum, 0, msg[i]);
				logmsg(WARN, buffer);
				commserr = 1;
				DEBUG fprintf(stderr, "%s\nBad packet: ", buffer);
				DEBUG dumpbuf();
			}
			commserr ++;
			if (commserr % 100 == 0) {
				sprintf(buffer, "WARN " PROGNAME " %d - %d non-header bytes", controllernum, commserr);
				logmsg(WARN, buffer);
				DEBUG fprintf(stderr, "%s\nBad packet: ", buffer);
				DEBUG dumpbuf();
			}
		}
		else {
			if (commserr) {
				sprintf(buffer, "INFO " PROGNAME " %d exiting comms error mode after %d non-header bytes", controllernum, commserr);
				logmsg(INFO, buffer);
				DEBUG fprintf(stderr, "%s\nBad packet: ", buffer);
				DEBUG dumpbuf();
				commserr = 0;
			}
		}
	
	int checksum = 0;
	for (i = 3; i < len + 7; i++)
		checksum += msg[i];
	if ((checksum & 0xFF) != msg[len + 7]) {
		sprintf(buffer, "WARN " PROGNAME " %d Checksum fails got %02x instead of %02x", controllernum, msg[len + 7], checksum);
		logmsg(WARN, buffer);
		return;
	}
	staticInfo.commandComplete = 1; // signal we have a complete packet
//	serbufindex = 0;
	
	// Silently set exponent to a valid value if it is provided as 11.
	if (index >= VARSTART && index <= VAREND && exp == 11) exp = exponent[index - VARSTART];

	
	if (index >= VARSTART && index <= VAREND) {	// If it's a value, check exponent.
		value = val * tentothe(exp);		// value may be zero due to underflow/overflow of exponent
		DEBUG2 fprintf(DEBUGFP, "%s Process message %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x (val %d) ", 
			getTime(), msg[0], msg[1], msg[2], msg[3], msg[4], msg[5], msg[6], msg[7], msg[8], msg[9], msg[10], val);
		if (exp < -3) {
			sprintf(buffer, "WARN " PROGNAME " %d Exponent underflow: %02x in message %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x (val %d)", 
					controllernum + currentInverter, exp, msg[0], msg[1], msg[2], msg[3], msg[4], msg[5], msg[6], msg[7], msg[8], msg[9], msg[10], val);
			logmsg(WARN, buffer);
			value = 0.0;
		} else if (exp > 10 && value > 0) {		// This is a "can't happen"
			sprintf(buffer, "WARN " PROGNAME " %d Exponent overflow: %02x in message %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x (val %d)", 
					controllernum + currentInverter, exp, msg[0], msg[1], msg[2], msg[3], msg[4], msg[5], msg[6], msg[7], msg[8], msg[9], msg[10], val);
			logmsg(WARN, buffer);
			value = 0.0;
		}
	} 
		
	staticInfo.awaitReply = 0;              // Normally it's a response we expect, so clear awaitReply
	/* Where required, we reset awaitReply to 1 */
	// 
	if (index >= VARSTART && index <= 0x2A) {		// Correctly hard-coded values for possible range of measured values
												// independant of the ones we actually query
		
		// NOTE
		// currentInverter is in range 0 .. servers-1 and is an index into inveter[] to get the actual
		// inverter number (invnum) which is in range 1 .. MAXINVERTERS (ie, 1-based addressing)
		
		// Bounds check on inverter[currentInverter];
		int invnum = inverter[currentInverter];
		if (invnum < 1 || invnum > MAXINVERTERS) {
			sprintf(buffer, "ERROR " PROGNAME " %d InverterNumber out of bounds: %d (Max is %d)", controllernum + invnum - 1, invnum, MAXINVERTERS);
			logmsg(ERROR, buffer);
		}
		float *valp = responseVal[invnum - 1];
		DEBUG2 fprintf(DEBUGFP, " responseVal[%d][%02d] to %f\n", currentInverter, index, value);
		
		// DANGER using index (validated above as in range VARSTART .. 0x2A into arrays declared as [VAREND - VARSTART + 1] which is 0..8
		
		if (index >= VARSTART && index <= VAREND)
			valp[index - VARSTART] = sanitycheck(value, index, valp[index - VARSTART], &count[invnum - 1][index - VARSTART]);
		else {
			sprintf(buffer, "WARN " PROGNAME " %d Ignoring invalid data index %d", controllernum + invnum - 1, index);
			logmsg(WARN, buffer);
		}
		staticInfo.awaitReply = 0;  
		if (++staticInfo.commandIndex > staticInfo.commandLimit) {
			// DEBUG fprintf(DEBUGFP, "Sequence Complete\n");
			staticInfo.sequenceComplete = 1;		// send data.
			if (dataFormat == old) 
				sprintf(buffer, "data 9 %.0f %.0f %.0f %.0f %.2f %.1f %.2f %.3f %.1f", valp[0],
				valp[1], valp[2], valp[3], valp[4], valp[5], valp[6], valp[7],valp[8]);
			else
				sprintf(buffer, "inverter watts:%.0f kwh:%.1f iac:%.2f vac:%.1f hz:%.3f idc:%.2f vdc:%.1f",
						valp[0], valp[1]/1000.0, valp[4], valp[5], valp[6], valp[7], valp[8]);
			// Bugfix -was looking at valp[3] - energy for year not energy for ever.

// WARNING complex logic.  If not all inverters are online, we iterate through a subset.  For example a 
// system with 3 inverters and only 2 inverters (1 and 3) are online, the Active Inverters message sets
// numinverters = 2, inverter[0] = 1 and inverter[1] = 3. 
// Need to check that we are not trying to send to a socket more than 'servers' which is the number
// declared on the command line.  Numinverters should always be less than or equal to this.
// This could go wrong if we declare our inverters (IG NO) not in strict order - for example 1, 2, 5.
// Prevetn this by checking that no inverter number is more than servers in the Active Inverter messages

			if (invnum > servers) {
				sprintf(buffer, "ERROR " PROGNAME " %d Trying to send data for inverter %d - max declared was %d", 
					controllernum + invnum - 1, invnum, servers);
				logmsg(ERROR, buffer);
				return;
			}
			DEBUG fprintf(stderr, "SEND[%d]: %s\n", invnum, buffer);
			sockSend(sockfd[invnum - 1], buffer);
			// Progress to next inverter or reset to first
			currentInverter++;
			if (currentInverter >= numInverters) currentInverter = 0;
			DEBUG2 fprintf(DEBUGFP, "Current inverter set to %d (%d) ", currentInverter, inverter[currentInverter]);
		}
	} else 
        switch (index) { // the response type
			case GETVERSION:                      // Version info
				staticInfo.sequenceComplete = 1;        // to trigger next sequence
				if (len == 8) {	// Directly addressed version
					sprintf(buffer, "INFO " PROGNAME " %d Type %s Version IFC:%02x.%02x.%02x SW:%02x.%02x.%02x.%02x",
							controllernum + currentInverter, msg[7] == 4 ? "IG+/RS485" : (msg[7] == 5 ? "IG TL/RS485" : "???"),
							msg[8], msg[9], msg[10], msg[11], msg[12], msg[13], msg[14]);
					logmsg(INFO, buffer);
					break;
				}
				if (len == 4) {	// Broadcast version
					systemType = msg[7];
					if (systemType < lastType) {
						DEBUG fprintf(stderr, "Setting system type '%s' (%d)\n", systemStr[systemType], systemType);
					}
					else {
						sprintf(buffer, "ERROR " PROGNAME " %d GetVersion got invalid system type as %d", 
								controllernum + currentInverter, systemType);
						logmsg(ERROR, buffer);
						systemType = datalogger;		// Should be safe enough
					}					
					sprintf(buffer ,"INFO " PROGNAME " %d Type %s Version %02x.%02x.%02x", controllernum + currentInverter, 
							systemStr[systemType], msg[8], msg[9], msg[10]);
					logmsg(INFO, buffer);
				}
                break;
			case GETDEVICETYPE:                      // Device type
		        staticInfo.sequenceComplete = 1;
				sprintf(buffer, "INFO " PROGNAME " %d Device Type %02x (%s)", controllernum + currentInverter, msg[7], deviceType(msg[7]));
				logmsg(INFO, buffer);
                break;
			case GETACTIVEINVERTERS:                      // Active inverters
				staticInfo.sequenceComplete = 1;
				DEBUG2 fprintf(DEBUGFP, "Activeinverters: inverterStatus %x prevInverterStatus %x\n", inverterStatus, prevInverterStatus);
				if (msg[3] == 0) {
					inverterStatus = 0;
					numInverters = 0;
					if (inverterStatus != prevInverterStatus) {
						sprintf(buffer, "WARN " PROGNAME " %d No active inverters", controllernum);
						logmsg(WARN, buffer);
					}
				} else {
					if (msg[3] <= MAXINVERTERS) {
						int i; char buf2[10];
						sprintf(buffer, "INFO " PROGNAME " %d %d Active inverters: ", controllernum, msg[3]);
						numInverters = msg[3];
						inverterStatus = 0;
						for (i = 0; i < msg[3]; i++) {
							sprintf(buf2, "%d ", inverter[i] = msg[7+i]);
							if (inverter[i] == 0) {
								inverter[i] = 1;
								if (!have_warned)	{
									have_warned = 1;
									logmsg(WARN, "WARN " PROGNAME " %d Correcting 0 to 1");
								}
							}
							if (inverter[i] == 34) {	// Strange glitch
								sprintf(buffer, "ERROR " PROGNAME " %d Got inverter number 34; discarding", controllernum);
								logmsg(ERROR, buffer);
								return;
							}
							if (inverter[i] > servers) {
								sprintf(buffer, "FATAL " PROGNAME " %d Got inverter number %d more than max(%d). Change IG-NR to below this.",
									controllernum, inverter[i], servers);
								logmsg(FATAL, buffer);
								return;
							}
							strcat(buffer, buf2);
							if (inverter[i] < 32) inverterStatus |= (1 << inverter[i]); // Watch out for overflow of int
						}
						if (inverterStatus != prevInverterStatus) // Require that at least one inverter is numbered less than 32!
							logmsg(INFO, buffer);
						DEBUG fprintf(DEBUGFP, "InverterStatus bitmask = %04x ", inverterStatus);
					}
					else {
						sprintf(buffer, "WARN " PROGNAME " %d Got invalid length for Active Inverters as %d", controllernum, msg[3]);
						logmsg(WARN, buffer);
					}
				}
				prevInverterStatus = inverterStatus;
					break;
			case SETERRORFORWARDING:		// Activate Error response.
			// For interface card easy, send 0D 02 55. For a Localnet system, send 0D (dayofmonth) 55.
			// In theory, the day of month should work, but it relies on having configured the date into the 
			// Datalogger.  This needs to be done using IG.Access.
			// The errorActivateState variable tracks progress through initialisation and then gets out of the
			// way in case we are issuing ErrorActivate commands interactively.
				staticInfo.sequenceComplete = 1;
				DEBUG fprintf(DEBUGFP, "ActivateError response to %d: 0x%02x eas=%d ", errorParam1, msg[7], errorActivateState);
				if (errorActivateState == easInit) {	// Response to initial ErrorActivate
					if (msg[7] == 0x55) {	// success
						errorActivateState = easComplete;
						sprintf(buffer, "INFO " PROGNAME " %d ActivateError successful on %d\n", controllernum, errorParam1);
						logmsg(INFO, buffer);
						break;
					} else {	// failed. Give up.
						errorActivateState = easComplete;
						sprintf(buffer, "WARN " PROGNAME " %d Activate Error Forwarding failed", controllernum);
						logmsg(WARN, buffer);
						break;
					}
				}		
				if (errorActivateState == easComplete) { // Response to interactive Error Activation
					if (msg[7] == 0x55) {	// success
						DEBUG fprintf(DEBUGFP, "ActivateError (interactive) successful\n");
						sprintf(buffer, "INFO " PROGNAME " %d Activate Error Forwarding succeeded", controllernum);
						logmsg(INFO, buffer);
						break;
					} else {	// failed.  This is a problem
						DEBUG fprintf(DEBUGFP, "ActivateError (interactive) failed\n");
						sprintf(buffer, "WARN " PROGNAME " %d Activate Error Forwarding failed", controllernum);
						logmsg(WARN, buffer);
						break;
					}
				}
					
				break;
			case SETERRORSENDING:		// Error Sending response
				// This is one byte per inverter. FF = ErrorSending activates; inverter number = not activated
				// Initial message is 1 2 3 .. servers (the -n parameter);
				staticInfo.sequenceComplete = 1;

				if (len > MAXINVERTERS + 1) {
					sprintf(buffer, "ERROR " PROGNAME " %d Errorsending: Got %d responses, more than MAXINVERTERS (%d)", 
							controllernum, len - 1, MAXINVERTERS);
					logmsg(ERROR, buffer);
					break;
				}
				if (len > servers + 1) {
					sprintf(buffer, "ERROR " PROGNAME " %d Errorsending: Got %d responses, more than configured (%d)", 
							controllernum, len - 1, servers);
					logmsg(ERROR, buffer);
					break;
				}
				char succeed[MAXINVERTERS * 3];
				char failed [MAXINVERTERS * 3];
				succeed[0] = failed[0] = 0;
				char buf2[6];
				sprintf(buffer, "INFO " PROGNAME " %d ErrorSending Activated: ", controllernum);
				for (i = 8; i < len + 7; i++) {
					sprintf(buf2, "%d ", i - 7);
					if (msg[i] == 0xff)
						strcat(succeed, buf2);
					else
						strcat(failed, buf2);
				}
				if (strlen(succeed) > 0) {
					strcat(buffer, "Succeeded: ");
					strcat(buffer, succeed);
				}
				if (strlen(failed) > 0) {
					strcat(buffer, "Failed: ");
					strcat(buffer, failed);
				}
				// Format of string is 1 2 ff ff where ff is success and 1, 2 are failure. 
				// Could be improved.
				logmsg(INFO, buffer);
				errorActivateState = easComplete;
				break;				
			case PROTOCOLERROR:		// Error response as inverter is off (night time)
				// 1.31 - change from ERROR to INFO
				sprintf(buffer, "INFO " PROGNAME " %d Protocol Error: Command 0x%02x %s - ignoring\n", 
						controllernum + currentInverter, msg[7], protocolError(msg[8]));
				logmsg(INFO, buffer);
				// TODO put code in here to handle a error response to 0D ActivateError command
				staticInfo.sequenceComplete = 1;
				break;	
			case ERRORSTATE:		// Error Message
			// TODO code to suppress extraneous messages
				sprintf(buffer, "WARN " PROGNAME " %d ErrorCode Dev/Opt:%d Number:%d Code:%d Extra:%d %s", 
						controllernum, msg[4], msg[5], val, msg[9], statusText(msg[9]));
				logmsg(WARN, buffer);
				break;
			default:                // unexpected response
                sprintf(buffer, "WARN " PROGNAME " %d Unexpected packet LEN %02x DEV %02x NUM %02x CMD %02x %02x %02x", 
					controllernum + currentInverter, msg[3], msg[4], msg[5], msg[6], msg[7], msg[8]);
                logmsg(WARN, buffer);
                break;
        }
};

/*****************/
/* PROCESSSOCKET */
/*****************/
int processSocket(void) {
	// Deal with commands from MCP.  Return to 0 to do a shutdown
	// Commands get added to the queue
	short int msglen, numread;
	char buffer[128];  // about 128 is good but rather excessive since longest message is 'truncate'
	char * cp = &buffer[0];
	int retries = NUMRETRIES;
	int num;
	
	if ((num = read(sockfd[0], &msglen, 2)) != 2) {
		sprintf(buffer, "WARN " PROGNAME " %d ProcessSocket Failed to read length from socket", controllernum);
		logmsg(WARN, buffer);
		fprintf(DEBUGFP, "ProcessSocket - read %d sockfd = %d", num, sockfd[0]);
		return 1;
	}
	msglen =  ntohs(msglen);
	while ((numread = read(sockfd[0], cp, msglen)) < msglen) {
		cp += numread;
		msglen -= numread;
		DEBUG printf("Only read %d, %d remaining\n", numread, msglen);
		if (--retries == 0) {
			sprintf(buffer, "WARN " PROGNAME " %d Timed out reading from server", controllernum);
			logmsg(WARN, buffer);
			return 1;
		}
		usleep(RETRYDELAY);
	}
	cp[numread] = '\0';     // terminate the buffer 
	DEBUG fprintf(DEBUGFP,"ProcessSocket: '%s'\n", buffer);
	
	if (strcasecmp(buffer, "exit") == 0)                                    /* exit */
		return 0;       // Terminate program
	else if (strcasecmp(buffer, "Ok") == 0)                                              /* Ok */
		return 1;       // Just acknowledgement
	else if (strcasecmp(buffer, "truncate") == 0) {                              /* truncate */
		if (logfp) {
			freopen(NULL, "w", logfp);
			sprintf(buffer, "INFO " PROGNAME " %d truncated log file", controllernum);
			logmsg(INFO, buffer);
		} else
			sprintf(buffer, "INFO " PROGNAME " %d Log file not truncated as it is not open", controllernum);
			logmsg(INFO, buffer);

		return 1;
	}
	else if (strcasecmp(buffer, "debug 0") == 0) {
		debug = 0; return 1;
	} else if (strcasecmp(buffer, "debug 1") ==0) {
		debug = 1; return 1;
	} else if (strcasecmp(buffer, "debug 2") ==0) {
		debug = 2; return 1;
	} else if (strcasecmp(buffer, "help") == 0) {
		logmsg(INFO, "INFO " PROGNAME " Available commands: GetSWVersion, GetDevType, GetActiveInverters, ActivateError xx yy, debug 0|1|2, exit");
		return 1;
	}
	
	// If it's a command, set SequenceComplete
	staticInfo.sequenceComplete = 1;
	// Check room avail in queue
	queue.top++;
	if (queue.top == QUEUESIZE) queue.top = 0;
	if (queue.top == queue.bottom) {
		sprintf(buffer, "WARN " PROGNAME " %d Queue full - ignoring command", controllernum);
		logmsg(WARN, buffer);

		return 1;
	}
	if (strcasecmp(buffer, "GetSWVersion") == 0) {                 /* GetSWVersion */
		queue.type[queue.top] = GetVersion;
		return 1;
	}
	if (strcasecmp(buffer, "GetDevType") == 0) {                  /* GetDeviceType */
		queue.type[queue.top] = GetDevType;
		return 1;
	}
	if (strcasecmp(buffer, "GetActiveInverters") == 0) {                /* GetActiveInverters */
		queue.type[queue.top] = GetActiveInverters;
		return 1;
	}
	if (strncasecmp(buffer, "ActivateError", 13) == 0) {		/* ActivateError */
		int num = sscanf(buffer+13, "%d %x", &errorParam1, &errorParam2);
		if (num == 0) {
			sprintf(buffer, "INFO " PROGNAME " %d No parameters supplied to ActivateError", controllernum);
			logmsg(INFO, buffer);
	//	Must still invoke an Activate Error as otherwise there will be a hole in the queue
		errorParam1 = 2;		// let's hope.
		}
		if (num == 1) errorParam2 = 0x55;
		DEBUG fprintf(DEBUGFP, "ActivateError with num %d params %d (%02x) %d (%02x)\n", 
			num, errorParam1, errorParam1, errorParam2, errorParam2);
		queue.type[queue.top] = ActivateError;
		return 1;
	}
	
	{ 
		char buffer2[192];
		sprintf(buffer2, "WARN " PROGNAME " %d Unknown message from server: ", controllernum);
		strcat(buffer2, buffer);
		logmsg(WARN, buffer2);  // Risk of loop: sending unknown message straight back to server
		// Undo increment of queue.top;
		queue.top--;
		if (queue.top <0) queue.top = 0;
	}
	return 1;
};

char * deviceType(int n) {
// Return a string for the Device Type
	switch(n) {
		case 0xfe: return "Fronius IG 15 (1300W)";
		case 0xfd: return "Fronius IG 20 (1800W)";
		case 0xfc: return "Fronius IG 30 (2500W)";
		case 0xfb: return "Fronius IG 30 DUMMY";
		case 0xfa: return "Fronius IG 40 (3500W)";
		case 0xf9: return "Fronius IG 60 (4600W)";
		case 0xf6: return "Fronius IG 300 (24000W)";
		case 0xf5: return "Fronius IG 400 (32000W)";
		case 0xf4: return "Fronius IG 500 (40000W)";
		case 0xf3: return "Fronius IG 60HV (4600W)";
		case 0xee: return "Fronius IG 2000";
		case 0xed: return "Fronius IG 3000";
		case 0xeb: return "Fronius IG 4000";
		case 0xea: return "Fronius IG 5100";
		case 0xe5: return "Fronius IG 2500LV";
		case 0xe3: return "Fronius IG 4500LV";
			// Added 1.22
		case 0xdf:	return "Fronius IG Plus 11.4-3 Delta";
		case 0xde:	return "Fronius IG Plus 11.4-1 UNI";
		case 0xdd:	return "Fronius IG Plus 10.0-1 UNI";
		case 0xdc:	return "Fronius IG Plus 7.5-1 UNI";
		case 0xdb:	return "Fronius IG Plus 6.0-1 UNI";
		case 0xda:	return "Fronius IG Plus 5.0-1 UNI";
		case 0xd9:	return "Fronius IG Plus 3.8-1 UNI";
		case 0xd8:	return "Fronius IG Plus 3.0-1 UNI";
		case 0xd7:	return "Fronius IG Plus 120-3 (10000W)";
		case 0xd6:	return "Fronius IG Plus 70-2 (6500W)";
		case 0xd5:	return "Fronius IG Plus 70-1 (6500)";
		case 0xd4:	return "Fronius IG Plus 35-1 (3500W)";
		case 0xd3:	return "Fronius IG Plus 150-3 (12000W)";
		case 0xd2:	return "Fronius IG Plus 100-2 (8000W)";
		case 0xd1:	return "Fronius IG Plus 100-1 (8000W)";
		case 0xd0:	return "Fronius IG Plus 50-1 (4000W)";
		case 0xcf:	return "Fronius IG Plus 12.0-3 WYE277";
		case 0xc1:	return "Fronius IG TL 3.6";
		case 0xc0:	return "Fronius IG TL 5.0";
		case 0xbf:	return "Fronius IG TL 4.0";
		case 0xbe:	return "Fronius IG TL 3.0";
			// Added 1.30
		case 0xb1:	return "Fronius IG PLus 35V-1";
		case 0xb0:	return "Fronius IG PLus 50V-1";
		case 0xaf:	return "Fronius IG PLus 70V-1";
		case 0xae:	return "Fronius IG PLus 70V-2";
		case 0xad:	return "Fronius IG PLus 100V-1";
		case 0xac:	return "Fronius IG PLus 100V-2";
		case 0xab:	return "Fronius IG PLus 120V-3";
		case 0xaa:	return "Fronius IG PLus 150V-3";
		case 0xa9:	return "Fronius IG PLus V 3.0-1 UNI";
		case 0xa8:	return "Fronius IG PLus V 3.8-1 UNI";
		case 0xa7:	return "Fronius IG PLus V 5.0-1 UNI";
		case 0xa6:	return "Fronius IG PLus V 6.0-1 UNI";
		case 0xa5:	return "Fronius IG PLus V 7.5-1 UNI";
		case 0xa4:	return "Fronius IG PLus V 10.0-1 UNI";
		case 0xa3:	return "Fronius IG PLus V 11.4-1 UNI";
		case 0xa2:	return "Fronius IG PLus V 11.4-3 DELTA";
		case 0xa1:	return "Fronius IG PLus V 12.0-3 WYE";
		case 0xa0:	return "Fronius IG PLus 50V-1 Dummy";
		case 0x9f:	return "Fronius IG PLus 100V-2 Dummy";
		case 0x9e:	return "Fronius IG PLus 150V-3 Dummy";
		case 0x9d:	return "Fronius IG PLus V 3.8-1 Dummy";
		case 0x9c:	return "Fronius IG PLus V 7.5-1 Dummy";
		case 0x9b:	return "Fronius IG PLus V 12.0-3 Dummy";
		case 0xbc:	return "Fronius CL 36.0";
		case 0xbd:	return "Fronius CL 48.0";
		case 0xc9:	return "Fronius CL 60.0";
		case 0xb9:	return "Fronius CL 36.0 WYE277";
		case 0xba:	return "Fronius CL 48.0 WYE277";
		case 0xbb:	return "Fronius CL 60.0 WYE277";
		case 0xb6:	return "Fronius CL 33.3 Delta";
		case 0xb7:	return "Fronius CL 44.4 Delta";
		case 0xb8:	return "Fronius CL 55.5 Delta";
		case 0x9a:	return "Fronius CL 60.0 Dummy";
		case 0x99:	return "Fronius CL 55.5 Delta Dummy";
		case 0x98:	return "Fronius CL 60.0 WYE277 Dummy";

		case 0xff: return "Fronius UNKNOWN";
		default:
			DEBUG fprintf(DEBUGFP, "Unknown Device Type %02x\n", n);
			return "Unknown Device Type";
	}
}

char * protocolError(int n) {
	switch(n) {
		case 1:  return "Unknown Command(1)";
		case 2:  return "Timeout(2)";
		case 3:  return "Incorrect data supplied(3)";
		case 4:  return "Command Queue full(4)";
		case 5:  return "Device not present(5)";
		case 6:  return "No response from device(6)";
		case 7:  return "Sensor Error(7)";
		case 8:  return "Sensor not active(8)";
		case 9:  return "Incorrect command(9)";
		case 10: return "Address Conflict(10)";
		default: return "Unknown error code";
	}
}			
			
////////////////
/* GETVERSION */
////////////////
char *getversion(void) {
// return pointer to version part of REVISION macro
	static char version[10] = "";	// Room for xxxx.yyyy
	if (!strlen(version)) {
		strcpy(version, REVISION+11);
		version[strlen(version)-2] = '\0';
	}
return version;
}

/////////////
/* GETTIME */
/////////////
char * getTime(void) {
// Return pointer to string in form yyyy/mm/dd hh:mm:ss
	static char buf [20];		// overrides global buf
	struct tm * mytm;
	time_t timeval;
	timeval = time(NULL);
	mytm = localtime(&timeval);
	sprintf(buf, "%4d/%02d/%02d %02d:%02d:%02d", mytm->tm_year + 1900, mytm->tm_mon+1,
		mytm->tm_mday, mytm->tm_hour, mytm->tm_min, mytm->tm_sec);
	return buf;
}

/***************/
/* SANITYCHECK */
/***************/
float sanitycheck(float value, int index, float prev, char * count) {
	// Check that supplied value is sensible. If not, return previous value but warn.
	// Also check that the index itself is sensible
    // Count is a pointer so it can be reset
	// First, if count = 2 or more, accept value.
	// 2.28 - look for sudden (downward) AC Voltage changes.
	if (*count > 2) {
		sprintf(buffer, "INFO " PROGNAME " %d Accepting value(%d) of %.1f as valid as count=%d although prev=%.1f",
				controllernum + currentInverter, index, value, *count, prev);
		logmsg(INFO, buffer);
		*count = 0;
		return value;
	}
	switch(index) {
		case 16:	// current power
			if (value > 10000) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely POWER NOW value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		case 17:	// Energy todate
		case 18:	// Energy today
		case 19: // Energy this year
			if ((prev > 0) && (value > prev + 10000)) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely ENERGY(%d) value of %.1f (prev %.1f)", controllernum + currentInverter, index, value, prev);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		case 20:	// AC Current
			if (value > 100) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely AC Current value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		case 21:	// AC VOLTAGE - permissiable range now includes 3-phase AC
			if (value > 550) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely AC Voltage value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			float vdc, idc;
			idc = responseVal[currentInverter][23 - VARSTART];
			vdc = responseVal[currentInverter][24 - VARSTART];
			// 2.28 - report sudden voltage reduction
			if (value < 200 && prev > 200 && vdc > 0.0) {
				sprintf(buffer, "WARN " PROGNAME " %d ACV = %.1f, previously %.1f. (Vdc %.1f Idc %.2f) Inverter shutdown (DC brownout)", 
						controllernum + currentInverter, value, prev, responseVal[currentInverter][24 - VARSTART], responseVal[currentInverter][23 - VARSTART]);
				logmsg(WARN, buffer);
				(*count)++;
				return value;	// Note NOT returning previous!
			}
			if (value > 200 && prev < 200 && vdc > 0.0) {
				sprintf(buffer, "WARN " PROGNAME " %d ACV = %.1f, previously %.1f. (Vdc %.1f Idc %.2f) Recovery from Inverter shutdown", 
						controllernum + currentInverter, value, prev, responseVal[currentInverter][24 - VARSTART], responseVal[currentInverter][23 - VARSTART]);
				logmsg(WARN, buffer);
				(*count)++;
				return value;	// Note NOT returning previous!
			}
			break;
		case 22:	// AC Frequency
			if (value > 100) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely AC Frequency value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		case 23:	// DC Current
			if (value > 100) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely DC Current value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		case 24:	// DC VOLTAGE
			if (value > 600) {
				sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely DC Voltage value of %.1f", controllernum + currentInverter, value);
				logmsg(WARN, buffer);
				(*count)++;
				return prev;
			}
			break;
		default:
			sprintf(buffer, "WARN " PROGNAME " %d Discarding unlikely index value of %d", controllernum + currentInverter, index);
			logmsg(WARN, buffer);
			(*count)++;
			return prev;
	}
	*count = 0;
	return value;
}

/**********/
/* GETBUF */
/**********/
int getbuf(int fd, int max, int mSec) {
	// Read up to max chars into supplied buf. Return number
	// of chars read or negative error code if applicable
	
	int ready, numtoread, now;
	fd_set readfd; 
	struct timeval timeout;
	FD_ZERO(&readfd);
	// numread = 0;
	numtoread = max;
	DEBUG2 fprintf(stderr, "Getbuf %d count=%d ", max ,data.count);
	
	while(1) {
		FD_SET(fd, &readfd);
		timeout.tv_sec = mSec / 1000;
		timeout.tv_usec = (mSec * 1000) % 1000000;	 // 0.5sec
		ready = select(fd + 1, &readfd, NULL, NULL, &timeout);
		DEBUG4 {
			gettimeofday(&timeout, NULL);
			fprintf(stderr, "%03ld.%03ld ", timeout.tv_sec%100, timeout.tv_usec / 1000);
		}
		if (ready == 0) 
			return data.count;		// timed out - return what we've got
		DEBUG4 fprintf(stderr, "Getbuf: before read1 ");
		now = read(fd, data.buf + data.count, 1);
		DEBUG4 fprintf(stderr, "After read1\n");
		DEBUG3 fprintf(stderr, "0x%02x ", data.buf[data.count]);
		if (now < 0)
			return now;
		if (now == 0) {
			fprintf(stderr, "ERROR fd was ready but got no data\n");
			fd = reopenSerial(fd, serialName, BAUD, 0, CS8, 1);
			//			usleep(1000000); // 1 Sec
			continue;
		}
		// For Elster don't care if SYNC byte found in middle of data
		/*		if (data.buf[data.count] == STARTSYNC && data.count != 0) {
		 sprintf(buffer, "WARN " PROGNAME " got 0x01 at position %d in packet", data.count);
		 logmsg(WARN, buffer);
		 return -1;
		 }
		 if (data.buf[data.count] == STARTSYNC)
		 return 0;
		 */		
		data.count += now;
		numtoread -= now;
		if (numtoread == 0) return data.count;
		if (numtoread < 0) {	// CANT HAPPEN
			fprintf(stderr, "ERROR buffer overflow - increase max from %d (numtoread = %d numread = %d)\n", 
					max, numtoread, data.count);
			return data.count;
			
		}
	}
}

/***********/
/* DUMPBUF */
/***********/
void dumpbuf() {
	int i;
	for (i = 0; i < data.count; i++) {
		fprintf(stderr, "%02x", data.buf[i]);
		if (i % 6 == 3)
			putc('-', stderr);
		else
			putc(' ', stderr);
	}
	putc('\n', stderr);
}

char * statusText(int n) {
	switch(n) {
			// Class 100 - typically temporary
		case 102:	return "AC Voltage too high";
		case 103:	return "AC Voltage too low";
		case 105:	return "AC frequency too high";
		case 106:	return "AC frequency too low";
		case 107:	return "NO AC Grid detected";
		case 108:	return "Islanding detected";
		case 112:	return "RMCU: Fault current in inverter";
		
			// Class 300 - non-permanent error during feed-in
		case 301:	return "Overcurrent AC";
		case 302:	return "Overcurrent DC";
		case 303:	return "Overtemperature DC side";
		case 304:	return "Overtemperature internally";
		case 305:	return "No power transfer to grid possible";
		case 306:	return "Power too low";
		case 307:	return "DC too low";
		case 308:	return "Intermediate circuit voltage too high";
		case 309:	return "DC inpt voltage too high";
			
			// Class 400 - probable hardware problem
		case 401:	return "No communication with power stage";
		case 406:
		case 407:	return "Error in temperature sensor";
		case 408:	return "Direct current feed-in";
		case 412:	return "Fixed voltage mode - out of range";
		case 416:	return "No communication between power stage and control unit";
		case 425:	return "Communication with power stage set not possible";
		case 426:	return "Intermediate circuit charging takes too long";
		case 427:	return "Power stage inoperative for too long";
		case 428:	return "Timeout error during connection";
		case 429:	return "Timeout error when disconneting";
		case 431:	return "Power stage software being updated";
		case 432:	return "Internal database error during power st allocation";
		case 433:	return "No dynamic indentification can be assigned to power stage";
		case 436:	return "Incorrect error information from power stage";
		case 437:	return "General troubleshooting in power stage";
		case 438:	return "Incorrect error information from power stage";
		case 442:	return "Power stage set not detected";
		case 443:	return "Energy transfer not possible";
		case 445:	return "Invalid power stage set configuration";
		case 447:	return "Solar module ground insulation error";
		case 450:	return "Error in Guard Control";
		case 451:	return "Guard Control memory faulty";
		case 452:	return "Communication between Guard and DSP interrupted";
		case 453:	return "Error in grid voltage recorded by Guard Control";
		case 454:	return "Error in grid frequency recorded by Guard Control";
		case 456:	return "Error in islanding check by Guard Control";
		case 457:	return "Grid relay defective";
		case 458:	return "DPS and Guard Control measure different RMCU values";
		case 459:	return "Measurement signal recording not possible for insulation test";
		case 460:	return "Reference source for DPS is outside tolerance";
		case 461:	return "Error in DSP memory";
		case 462:	return "Error in DC-feed monitoring routine";
		case 463:	return "AC polarity inverter";
		case 474:	return "RMCU sensor is defective";
		case 475:	return "Error in safety relay";
		case 476:	return "Internal component defective";
			
			// Class 500 - limitation in greed feed
		case 509:	return "No feed-in in last 24 hours - snow on panels?";
		case 515:	return "No communication with string monitor";
		case 516:	return "No communication with memory unit";
		case 517:	return "Power derating due to excessive temperature";
		case 518:	return "Internal DSP malfunction";
			
			// Class 700 - inverter control and interface
		default:	return "(No message available)";
	}
}

