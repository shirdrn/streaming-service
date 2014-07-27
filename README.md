##What's streaming-service

streaming-service is aimed at data collecting for realtime application scenario, not limit to log collection. streaming-service owns 2 components, which should be deployed separately:

* streaming-agent
The agent component should be running in the nodes where the data are being generated, and the daemon process always monitors changes of the data files configured. Once data lines of the data files are appended to the end of entire files, a push thread could read them out and push to the remote peer, the streaming-server.

* streaming-server
The server component is as the role of remote peer for receiving pushed data lines from multiple the streaming-agent side components, and then the collected data can be dospatched to the real processing components which can be configureable and flexible according to the business need.


