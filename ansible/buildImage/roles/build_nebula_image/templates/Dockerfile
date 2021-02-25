FROM {{base_image_name}}:{{base_image_tag}}
MAINTAINER Jiacheng Yao "jc07.yao@gmail.com"

USER root

ADD .bashrc /root/.bashrc
ADD .aliases /root/.aliases
ADD .gitattributes /root/.gitattributes
ADD .gitconfig /root/.gitconfig
ADD .gitignore /root/.gitignore

RUN echo nameserver 8.8.8.8 > /etc/resolv.conf

RUN apt-get update -qq && DEBIAN_FRONTEND="noninteractive" apt-get install -qq \
  clang \
  cmake \
  libdwarf-dev \
  libdwarf1 \
  llvm-dev \
  binutils-dev \
  libdw-dev \
  libboost-all-dev \
  liblog4cxx-dev \
  libcpprest-dev \
  libssl-dev \
  clang-format \
  librdkafka1 \
  librdkafka++1 \
  librdkafka-dev \
  libeigen3-dev \
  libzmqpp-dev \
  libmbedtls-dev \
  git \
  wget \
  python3.8 \
  z3 \
  tar \
  doxygen \
  graphviz \
  software-properties-common \
  && add-apt-repository ppa:open62541-team/ppa && \
  apt-get update -qq && \
  apt-get install libopen62541-1-dev -qq \
  && apt-get clean -qq

RUN cd ${HOME} && git clone https://github.com/eclipse/paho.mqtt.c.git && \
  cd paho.mqtt.c && git checkout v1.3.8 && \
  cmake -Bbuild -H. -DPAHO_ENABLE_TESTING=OFF \
  -DPAHO_BUILD_STATIC=ON \
  -DPAHO_WITH_SSL=ON \
  -DPAHO_HIGH_PERFORMANCE=ON && \
  cmake --build build/ --target install && \
  ldconfig && cd ${HOME} && rm -rf paho.mqtt.c && \
  git clone https://github.com/eclipse/paho.mqtt.cpp && cd paho.mqtt.cpp && \
  cmake -Bbuild -H. -DPAHO_BUILD_STATIC=ON -DPAHO_BUILD_DOCUMENTATION=TRUE -DPAHO_BUILD_SAMPLES=TRUE && \
  cmake --build build/ --target install && ldconfig && cd ${HOME} && rm -rf paho.mqtt.cpp

RUN cd ${HOME} && git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --jobs 1 && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && \
  make -j2 && make install && cd ${HOME} && rm -rf grpc && \
  cd ${HOME} && git clone --single-branch --branch v0.3.1 https://github.com/mfontanini/cppkafka.git && \
  cd cppkafka && mkdir -p build && cd build && \
  cmake -DCPPKAFKA_BUILD_SHARED=ON -DCPPKAFKA_DISABLE_TESTS=ON -DCPPKAFKA_DISABLE_EXAMPLES=ON -DCPPKAFKA_RDKAFKA_STATIC_LIB=OFF .. && \
  make -j -w && make install && cd ${HOME} && rm -rf cppkafka* && \
  apt-get purge -y wget && apt autoremove -y

# Create an application user
RUN groupadd -r {{app_user}} && useradd -r -m -g {{app_user}} {{app_user}}

# Create necessary directories
RUN mkdir {{ app_root_dir }}
RUN chown {{app_user}}:{{app_user}} -R {{app_root_dir}}

# gittoken extracted from {{github_token}} in group_vars/private.yml
ARG gittoken
RUN git clone -b {{nebula_build_repo_branch}} --single-branch \
  https://jiacheng-yao:$gittoken@github.com/nebulastream/nebulastream.git {{app_root_dir}}/nebulastream/

ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

WORKDIR {{app_root_dir}}/

RUN mkdir {{ app_root_dir }}/volume/
VOLUME ["{{app_root_dir}}/volume"]

EXPOSE 22

# prep init
ADD init.sh /usr/local/bin/init.sh
# COPY init.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init.sh
CMD ["init.sh"]
