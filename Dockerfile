FROM dev.exactspace.co/python3.8-base-es2:r1
RUN mkdir /src
COPY power.py /src/
COPY tbwes.py /src/
COPY index.py /src/
COPY . /src/
COPY main /src/
RUN chmod +X /src/power.py
RUN chmod +X /src/tbwes.py
RUN chmod +x /src/main
RUN chmod +x /src/index.py
RUN chmod +x /src/*
WORKDIR /src
ENTRYPOINT ["./main"]
