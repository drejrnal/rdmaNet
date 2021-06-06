//
// Created by luoxiYun on 2021/1/3.
//

#ifndef LOKISERVER_LOG_H
#define LOKISERVER_LOG_H

#include <stdarg.h>

#define LOG_DEBUG_TYPE 0
#define LOG_MSG_TYPE 1
#define LOG_WARN_TYPE 2
#define LOG_ERR_TYPE 3

enum log_type{
    DEBUG, MSG, WARN, ERR
};


void rdsni_log(enum log_type, const char *msg);
void rdsni_logx(enum log_type, const char *errstr, const char *fmt, va_list);
void rdsni_msgx(const char *fmt, ...);
void rdsni_debugx(const char *fmt, ...);

#endif //LOKISERVER_LOG_H
