# Test coverage in docker container
## run build images

In the project folder, 

1. Build prerequisite image:
    ```shell
    docker build . -f prerequisite.dockerfile -t scupt-raft-prerequisite:latest
    ```

2. Build test image:
    ```shell
    docker build . -f test.dockerfile -t scupt-raft-test:latest
    ```

3. Run container web server:
    ```shell
    docker run --name scupt-raft-test --publish 8000:8000 --detach scupt-raft-cov
    ```

    See http://127.0.0.1:8000/ for coverage report
