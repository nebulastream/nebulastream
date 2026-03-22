/*
Copyright (c) 2009-2020 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

/* This contains general purpose utility functions that are not specific to
 * Mosquitto/MQTT features. */

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WIN32
#  include <winsock2.h>
#  include <aclapi.h>
#  include <io.h>
#  include <lmcons.h>
#  include <fcntl.h>
#  define PATH_MAX MAX_PATH
#else
#  include <sys/stat.h>
#  include <pwd.h>
#  include <grp.h>
#  include <unistd.h>
#endif

#include "misc_mosq.h"
#include "logging_mosq.h"


FILE *mosquitto__fopen(const char *path, const char *mode, bool restrict_read)
{
#ifdef WIN32
	char buf[4096];
	int rc;
	int flags = 0;

	rc = ExpandEnvironmentStringsA(path, buf, 4096);
	if(rc == 0 || rc > 4096){
		return NULL;
	}else{
		if (restrict_read) {
			HANDLE hfile;
			SECURITY_ATTRIBUTES sec;
			EXPLICIT_ACCESS_A ea;
			PACL pacl = NULL;
			char username[UNLEN + 1];
			DWORD ulen = UNLEN;
			SECURITY_DESCRIPTOR sd;
			DWORD dwCreationDisposition;
			DWORD dwShareMode;
			int fd;
			FILE *fptr;

			switch(mode[0]){
				case 'a':
					dwCreationDisposition = OPEN_ALWAYS;
					dwShareMode = GENERIC_WRITE;
					flags = _O_APPEND;
					break;
				case 'r':
					dwCreationDisposition = OPEN_EXISTING;
					dwShareMode = GENERIC_READ;
					flags = _O_RDONLY;
					break;
				case 'w':
					dwCreationDisposition = CREATE_ALWAYS;
					dwShareMode = GENERIC_WRITE;
					break;
				default:
					return NULL;
			}
			if(mode[1] == '+'){
				dwShareMode = GENERIC_READ | GENERIC_WRITE;
			}

			GetUserNameA(username, &ulen);
			if (!InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION)) {
				return NULL;
			}
			BuildExplicitAccessWithNameA(&ea, username, GENERIC_ALL, SET_ACCESS, NO_INHERITANCE);
			if (SetEntriesInAclA(1, &ea, NULL, &pacl) != ERROR_SUCCESS) {
				return NULL;
			}
			if (!SetSecurityDescriptorDacl(&sd, TRUE, pacl, FALSE)) {
				LocalFree(pacl);
				return NULL;
			}

			memset(&sec, 0, sizeof(sec));
			sec.nLength = sizeof(SECURITY_ATTRIBUTES);
			sec.bInheritHandle = FALSE;
			sec.lpSecurityDescriptor = &sd;

			hfile = CreateFileA(buf, dwShareMode, FILE_SHARE_READ,
				&sec,
				dwCreationDisposition,
				FILE_ATTRIBUTE_NORMAL,
				NULL);

			LocalFree(pacl);

			fd = _open_osfhandle((intptr_t)hfile, flags);
			if (fd < 0) {
				return NULL;
			}

			fptr = _fdopen(fd, mode);
			if (!fptr) {
				_close(fd);
				return NULL;
			}
			if(mode[0] == 'a'){
				fseek(fptr, 0, SEEK_END);
			}
			return fptr;

		}else {
			return fopen(buf, mode);
		}
	}
#else
	FILE *fptr;
	struct stat statbuf;

	if (restrict_read) {
		mode_t old_mask;

		old_mask = umask(0077);
		fptr = fopen(path, mode);
		umask(old_mask);
	}else{
		fptr = fopen(path, mode);
	}
	if(!fptr) return NULL;

	if(fstat(fileno(fptr), &statbuf) < 0){
		fclose(fptr);
		return NULL;
	}

	if(restrict_read){
		if(statbuf.st_mode & S_IRWXO){
#ifdef WITH_BROKER
			log__printf(NULL, MOSQ_LOG_WARNING,
#else
			fprintf(stderr,
#endif
					"Warning: File %s has world readable permissions. Future versions will refuse to load this file.\n"
					"To fix this, use `chmod 0700 %s`.",
					path, path);
#if 0
			return NULL;
#endif
		}
		if(statbuf.st_uid != getuid()){
			char buf[4096];
			struct passwd pw, *result;

			getpwuid_r(getuid(), &pw, buf, sizeof(buf), &result);
			if(result){
#ifdef WITH_BROKER
				log__printf(NULL, MOSQ_LOG_WARNING,
#else
				fprintf(stderr,
#endif
						"Warning: File %s owner is not %s. Future versions will refuse to load this file."
						"To fix this, use `chown %s %s`.",
						path, result->pw_name, result->pw_name, path);
			}
#if 0
			// Future version
			return NULL;
#endif
		}
		if(statbuf.st_gid != getgid()){
			char buf[4096];
			struct group grp, *result;

			getgrgid_r(getgid(), &grp, buf, sizeof(buf), &result);
			if(result){
#ifdef WITH_BROKER
				log__printf(NULL, MOSQ_LOG_WARNING,
#else
				fprintf(stderr,
#endif
						"Warning: File %s group is not %s. Future versions will refuse to load this file.",
						path, result->gr_name);
			}
#if 0
			// Future version
			return NULL
#endif
		}
	}


	if(!S_ISREG(statbuf.st_mode) && !S_ISLNK(statbuf.st_mode)){
#ifdef WITH_BROKER
		log__printf(NULL, MOSQ_LOG_ERR, "Error: %s is not a file.", path);
#endif
		fclose(fptr);
		return NULL;
	}
	return fptr;
#endif
}


char *misc__trimblanks(char *str)
{
	char *endptr;

	if(str == NULL) return NULL;

	while(isspace(str[0])){
		str++;
	}
	endptr = &str[strlen(str)-1];
	while(endptr > str && isspace(endptr[0])){
		endptr[0] = '\0';
		endptr--;
	}
	return str;
}


char *fgets_extending(char **buf, int *buflen, FILE *stream)
{
	char *rc;
	char endchar;
	int offset = 0;
	char *newbuf;
	size_t len;

	if(stream == NULL || buf == NULL || buflen == NULL || *buflen < 1){
		return NULL;
	}

	do{
		rc = fgets(&((*buf)[offset]), (*buflen)-offset, stream);
		if(feof(stream) || rc == NULL){
			return rc;
		}

		len = strlen(*buf);
		if(len == 0){
			return rc;
		}
		endchar = (*buf)[len-1];
		if(endchar == '\n'){
			return rc;
		}
		/* No EOL char found, so extend buffer */
		offset = (*buflen)-1;
		*buflen += 1000;
		newbuf = realloc(*buf, (size_t)*buflen);
		if(!newbuf){
			return NULL;
		}
		*buf = newbuf;
	}while(1);
}
