#import logging
import uvicorn


#logging.basicConfig(
    #level=logging.INFO,
    #format='%(asctime)s %(levelname)s %(message)s',
    #datefmt='%Y-%m-%d %H:%M:%S',
    #handlers=[
        #logging.FileHandler('/var/log/api1.log'),
        #logging.StreamHandler()  # Esto envía al stdout (visible en journalctl)
    #]
#)

if __name__ == "__main__":
    uvicorn.run("server.app:app", host="0.0.0.0", port=9050, reload=True)
