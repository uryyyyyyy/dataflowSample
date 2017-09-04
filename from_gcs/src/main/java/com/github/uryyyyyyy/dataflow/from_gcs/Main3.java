package com.github.uryyyyyyy.dataflow.from_gcs;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.IOException;

public class Main3 {

  public static void main(String[] args) throws IOException {
    GoogleCredentials credential = GoogleCredentials.getApplicationDefault();
    Storage storage = StorageOptions.newBuilder()
      .setCredentials(credential)
      .setProjectId("uryyyyyyy-sandbox")
      .build()
      .getService();

    // The name for the new bucket
    String bucketName = "dataflow-uryyyyyyy-sandbox";

    Bucket bucket = storage.get(bucketName);
    System.out.printf("Bucket %s created.%n", bucket.getName());
  }
}