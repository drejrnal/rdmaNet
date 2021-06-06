//
// Created by luoxiYun on 2021/1/3.
//

#include "log.h"
#include <stdio.h>
#include <string.h>

#define MAXLINE 4096

void rdsni_log(enum log_type type, const char *msg){
    const char *LOGGING_TYPE;
    switch (type) {
        case DEBUG:
            LOGGING_TYPE = "debug";
            break;
        case MSG:
            LOGGING_TYPE = "msg";
            break;
        case WARN:
            LOGGING_TYPE = "warn";
            break;
        case ERR:
            LOGGING_TYPE = "error";
            break;
        default:
            LOGGING_TYPE = "???";
            break;
    }
    fprintf(stdout, "[%s:%s]\n", LOGGING_TYPE, msg);
}

void rdsni_logx(enum log_type type, const char *errstr,
        const char *fmt, va_list vaList){
    char buf[1024];
    size_t len;

    if( fmt != NULL ){
        vsnprintf(buf, sizeof(buf), fmt, vaList);
    }else
        buf[0] = '\0';
    if(errstr){
        len = strlen(buf);
        if( len < sizeof(buf) - 3){
            snprintf(buf+len, sizeof(buf) - len, ": %s", errstr);
        }
    }

    rdsni_log(type, buf);
}

void rdsni_msgx(const char *fmt, ...){
    va_list vaList;
    va_start(vaList, fmt);
    rdsni_logx(MSG, NULL, fmt, vaList);
    va_end(vaList);
}

void rdsni_debugx(const char *fmt, ...){
    va_list vaList;
    va_start(vaList, fmt);
    rdsni_logx(DEBUG, NULL, fmt, vaList);
    va_end(vaList);
}
