# nodetest

Available options:
- --debug - debug level. The third level is the top;
- --numWorkers - bumber of spawned workers. Must be more than 0;
- --getErrors - show errors;
- --test - launch the test with publishing of 1000000 messages.


Command to show and clear error log:
    node index.js --getErrors
    
Command to start single worker:
    node index.js  --debug 3
    
Command to start several workers:
    node index.js *--numWorkers 5*
    
Command to start a test:
    node index.js  *--test* --numWorkers 5 --debug 1
