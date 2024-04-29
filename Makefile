.PHONY: build schedule-minio schedule-elastic-minio schedule-elastic-web


CURRENT_DIR := $(shell pwd)

LOGROTATE_CONF := /etc/logrotate.d/bgptools-combine

LOG_PATH := /var/log/bgptools-combine

define LOGROTATE_CONF_CONTENT
$(LOG_PATH)/*.log {
    rotate 5
    weekly
    missingok
    notifempty
    compress
    create 644 root root
}
endef

export LOGROTATE_CONF_CONTENT

all:build

init:
	-mkdir $(LOG_PATH)
	@echo "$$LOGROTATE_CONF_CONTENT" > $(LOGROTATE_CONF)

build:init
	-rm bgptools-combine
	@go build -o bgptools-combine

schedule-minio:
	@(crontab -l ; echo "22 5 * * * cd $(CURRENT_DIR); ./bgptools-combine minio") | crontab -

schedule-elastic-minio:
	@(crontab -l ; echo "22 5 * * * cd $(CURRENT_DIR); ./bgptools-combine elastic -i minio") | crontab -

schedule-elastic-web:
	@(crontab -l ; echo "22 5 * * * cd $(CURRENT_DIR); ./bgptools-combine elastic -i web") | crontab -