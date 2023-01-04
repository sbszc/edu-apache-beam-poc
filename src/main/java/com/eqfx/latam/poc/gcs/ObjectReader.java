package com.eqfx.latam.poc.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public interface ObjectReader {
    static void read(String bucketName, String filename) throws IOException {
        StorageOptions storageOptions = StorageOptions.getDefaultInstance();
        Storage service = storageOptions.getService();
        Blob blob = service.get(bucketName, filename);
        ReadChannel readChannel = blob.reader();
        Reader reader = Channels.newReader(readChannel, StandardCharsets.UTF_8);

        CSVParser records = CSVParser.parse(reader, CSVFormat.DEFAULT);
        records.stream().map(e->e.get("ProductId")).forEach(System.out::println);
    }

    static void main(String[] args) throws IOException {
        ObjectReader.read("cedar-router-beam-poc-kms-2","Case_1_Raw_Data_E.csv");
    }
}
