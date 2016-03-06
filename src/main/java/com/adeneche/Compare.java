package com.adeneche;

import com.adeneche.metadata.Metadata;
import com.adeneche.metadata.Metadata.MetadataHeader;
import com.adeneche.metadata.Metadata.MetadataFiles;
import com.google.protobuf.CodedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

public class Compare {

  private static class MetadataHolder {
    MetadataHeader header;
    MetadataFiles files;

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (obj == this) return true;
      MetadataHolder holder = (MetadataHolder) obj;
      return Objects.equals(header, holder.header) && Objects.equals(files, holder.files);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("USAGE file1 file2");
      System.exit(-1);
    }

    {
      boolean assertOn = false;
      assert assertOn = true;

      if (!assertOn) {
        throw new RuntimeException("assertions are not enabled");
      }
    }

    MetadataHolder metadata1 = parse(args[0]);
    MetadataHolder metadata2 = parse(args[1]);

    assert metadata1.equals(metadata2): "metadata is different";
  }

  private static MetadataHolder parse(final String filename) throws IOException {
    CodedInputStream codedStream = CodedInputStream.newInstance(new FileInputStream(filename));
    MetadataHolder holder = new MetadataHolder();

    // read header
    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    holder.header = Metadata.MetadataHeader.parseFrom(codedStream);
    codedStream.popLimit(limit);

    // read files
    length = codedStream.readRawVarint32();
    limit = codedStream.pushLimit(length);
    holder.files = Metadata.MetadataFiles.parseFrom(codedStream);
    codedStream.popLimit(limit);

    return holder;
  }
}
