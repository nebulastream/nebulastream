FROM ubuntu:20.04
MAINTAINER Jiacheng Yao "jc07.yao@gmail.com"

USER root

ADD .bashrc /root/.bashrc
ADD .aliases /root/.aliases
ADD .gitattributes /root/.gitattributes
ADD .gitconfig /root/.gitconfig
ADD .gitignore /root/.gitignore

RUN echo nameserver 8.8.8.8 > /etc/resolv.conf
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y

# apt-gets
RUN apt-get update
RUN apt-get install -y wget curl git nano less bash git bash-completion libpq-dev sudo
RUN apt-get update && apt install build-essential supervisor vim -y

ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Clean up APT in the end
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# prep init
ADD init.sh /usr/local/bin/init.sh
# COPY init.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init.sh
CMD ["init.sh"]
