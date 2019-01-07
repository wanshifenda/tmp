const google = require('googleapis'); 

exports.runDataprep = (event, callback) => {
  const file = event.data;
  var i = file.name.lastIndexOf('.');
  const file_ext = (i < 0) ? '' : file.name.substr(i);
  console.log(`Detecting new file uploaded: ${file.bucket}/${file.name}`);

  if(file_ext == '.gz' && file.name.indexOf('Chartbeat') > -1){
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
        var currentdate = new Date();
        var jobName = "prep-flow-gcs-bq-" + currentdate.getFullYear() + "-" + currentdate.getMonth() + "-" + currentdate.getDay() + "-"+ currentdate.getHours() + "-" + currentdate.getMinutes() + "-" + currentdate.getSeconds();

        dataflow.projects.templates.create({
          "projectId": projectId,
          "resource": {
            "gcsPath": "gs://dev-staging-bucket/terence.shi@bbc.co.uk/temp/cloud-dataprep-chartbeat-test-1392644-by-terenceshi_template",
            "jobName": `${jobName}`,
            "parameters": {
              "customGcsTempLocation": "gs://dev-staging-bucket/terence.shi@bbc.co.uk/temp",
              "outputLocations": "{\"location1\": \"bbc-data-warehouse-dev-7214:chartbeat.sessions\"}",
              "inputLocations": `{"location1": "gs://${file.bucket}/${file.name}"}`
            },
            "environment": {
              "tempLocation": "gs://dev-staging-bucket/temp/dataflow-cloud-functions-dataprep"
            }
          }
        });
      });
    });
  } else {
    console.error(`Err was: wrong ChartBeat file (${file.name})`);
  }
};