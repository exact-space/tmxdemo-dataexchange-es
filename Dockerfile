FROM dev.exactspace.co/python3.8-base-es2:r1
RUN mkdir /src
COPY powerMqtt.py /src/
COPY index.py /src/
COPY .py /src/
COPY main /src/
RUN chmod +X /src/powerMqtt.py
RUN chmod +x /src/main
RUN chmod +x /src/index.py
RUN chmod +x /src/*
WORKDIR /src
ENTRYPOINT ["./main"]
