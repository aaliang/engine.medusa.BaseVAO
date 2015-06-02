# engine.medusa.BaseVAO
Scala OrientDb wrap accessor

This is a quick generic DAO pattern for OrientDB targeting Scala. It wraps the native Java API.

Support for inserting, and selecting for now. Some return {scala.concurrent.Future}s some do not.

you will need an src/resources/application.conf to have a section

```
orientdb {
  iUri = "remote:localhost/somedb"
  userName = "root"
  password = "admin"
}
```
