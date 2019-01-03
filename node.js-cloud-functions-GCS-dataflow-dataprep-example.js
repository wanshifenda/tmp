const google = require('googleapis'); 

exports.runDataprep = (event, callback) => {
  google.auth.getApplicationDefault(function (err, authClient) {
    if (err) {
      throw err;
    }

    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
      authClient = authClient.createScoped([
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/userinfo.email"
      ]); 
    }

    google.auth.getDefaultProjectId(function(err, projectId) {
      if (err || !projectId) {
        console.error(`Problems getting projectId (${projectId}). Err was: `, err);
        throw err;
      }

      const dataflow = google.dataflow({ version: "v1b3", auth: authClient });

      // const file = event.data;
      //using dynamic file gs://${file.bucket}/${file.name}
      
      dataflow.projects.templates.create({
        "projectId": projectId,
        "resource": {
          "gcsPath": "gs://dev-staging-bucket/terence.shi@bbc.co.uk/temp/cloud-dataprep-hello-1359366-by-terenceshi_template",
          "jobName": "hello-dataprep-from-google-functions",
          "parameters": {
            "inputLocations" : "{\"location1\":\"gs://michael_sandbox/bbc_tw_ip_20160410.csv\"}",
            "outputLocations": "{\"location1\": \"gs://terence_bucket/output/bbc_tw_ip_20160410_N\"}"
          },
          "environment": {
            "tempLocation": "gs://dev-staging-bucket/temp/dataflow-cloud-functions-dataprep"
          }
        }
      });
    });
  });
};