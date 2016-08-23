morphlines: [
  {
    id: morphline1

    importCommands : ["org.kitesdk.**", "com.stratio.morphlines.**"]

    commands: [
      {
        readLine {
          charset: UTF-8
        }
      }
      {
        setValues {
          _attachment_body: "@{message}"
        }
      }
      {
        java {
          code: """
            record.replaceValues("_attachment_body", record.getFirstValue("_attachment_body").toString().getBytes(Charsets.UTF_8));
            return child.process(record);
            """
        }
      }
      {
        generateUUID {
          field: id
        }
      }
      {
        grok {
          dictionaryFiles: [/opt/apache-flume-1.6.0-bin/conf/grok-dictionaries/grok-patterns]
          expressions: {
            message: """%{IPORHOST:log_host} - %{USER:log_user} \[%{HTTPDATE:log_date}\] \"%{DATA:log_http_method} %{DATA:log_url_path} %{DATA:log_http_version}\" %{NONNEGINT:log_http_code} %{DATA:log_bytes_returned}"""
          }
        }
      }

      {
        convertTimestamp {
          field: log_date
          inputFormats: ["dd/MMM/yyyy:HH:mm:ss Z"]
          inputTimezone: UTC
          outputFormat: "yyyy-MM-dd'T'HH:mm:ss'Z'" 
          outputTimezone: UTC
        }
      }

      { logInfo { format : "Output record: {}", args : ["@{}"] } }

      # Consume the output record of the previous command, transform it
      # and pipe the record downstream.
      #
      # This command deletes record fields that are unknown to Solr
      # schema.xml. Recall that Solr throws an exception on any attempt to
      # load a document that contains a field that isn't specified in
      # schema.xml.
      {
        sanitizeUnknownSolrFields {
          # Location from which to fetch Solr schema
          solrLocator : {            
            collection : flumeSolrSink1       # Name of solr collection
            zkHost : "10.0.20.47:2181,10.0.10.243:2181,10.0.10.244:2181/solr" # ZooKeeper ensemble
          }
        }
      }

      # log the record at INFO level to SLF4J
      { logInfo { format : "output record: {}", args : ["@{}"] } }

      # load the record into a Solr server or MapReduce Reducer
      {
        loadSolr {
          solrLocator : {            
            collection : flumeSolrSink1       # Name of solr collection
            zkHost : "10.0.20.47:2181,10.0.10.243:2181,10.0.10.244:2181/solr" # ZooKeeper ensemble
          }
        }
      }

    ]
  }
]
