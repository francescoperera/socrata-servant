# socrata-servant
socrata-servant is a command line tool that allows you download data from all datasets with a given column. The only
required input is a valid Socrata column_field_name. SBT is required
to use socrata-servant.

## Required
#### sbt
download SBT from here <http://www.scala-sbt.org/>

## Compile
```sbt compile```

##Run
```sbt "run column_field_name"```

##Output
A file named column_field_name_socrata.json will be saved to Datalogue's S3 bucket.

###Example

``` sbt "run fullname"```

The file will be saved to S3 as fullname_socrata.json. The file content will be new - line delimited json.


```
{"fullname":"Sig Rogich"}
{"fullname":"Justice Myron E. Leavitt"}
{"fullname":"Brian and Teri Cram"}
{"fullname":"Mario C. and JoAnne Monaco"}
{"fullname":"Jack Lund Schofield"}
{"fullname":"Kay Carl"}
{"fullname":"Marshall C. Darnell"}
{"fullname":"Neil C. Twitchell"}

```



