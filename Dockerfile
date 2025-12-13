FROM debian:bullseye

ADD ./target/release/msd /usr/bin/msd

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

ENV MSD_DB_PATH=/opt/msd
ENV MSD_WORKERS=8
ENV MSD_LISTEN_ADDR=0.0.0.0:50510

CMD ["msd", "server" ]