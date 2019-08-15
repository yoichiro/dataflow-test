# Cloud Dataflow Test Project

This is a test project to use Dataflow with Pub/Sub and Datastore.

## Prerequisite

* You need to create a topic on Pub/Sub.
* You need to create a kind on Datastore.
* You need to issue and download a security key file and to specify its path to `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
* You need to specify your GCP project ID to `PROJECT_ID` environment variable.

## How to use

**Server**

```bash
$ ./deploy.sh
```

**Client**

```bash
$ ./client.sh 1 1 u1
```

* Argument 1: jobNum
* Argument 2: rowNum
* Argument 3: userId
