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
          field: log_id
        }
      }
      {
        grok {
          dictionaryFiles: [conf/grok-dictionaries/grok-patterns]
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
          outputFormat: "yyyy-MM-dd HH:mm:ss" 
          outputTimezone: UTC
        }
      }

      {
         setValues{
	        _attachment_body:[],
                message:[],
                log_bytes_returned:[]
                topic:[]
		header:{"log_id":"@log_id","log_host":"@log_host","log_user":"@log_user", "log_date":"@log_date", "log_http_method":"@log_http_method", "log_url_path":"@log_url_path", "log_http_version":"@log_http_version", "log_http_code":"@log_http_code"}
         }

      }

      { logInfo { format : "Output record: {}", args : ["@{}"] } }

    ]
  }
]
