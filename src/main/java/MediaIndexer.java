import org.jaudiotagger.FileConstants;
import org.jaudiotagger.audio.AudioFile;
import org.jaudiotagger.audio.AudioFileIO;
import org.jaudiotagger.audio.exceptions.CannotReadException;
import org.jaudiotagger.audio.exceptions.InvalidAudioFrameException;
import org.jaudiotagger.audio.exceptions.ReadOnlyFileException;
import org.jaudiotagger.tag.FieldKey;
import org.jaudiotagger.tag.Tag;
import org.jaudiotagger.tag.TagException;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class MediaIndexer {
    private static String url;

    public static void main(String[] args) {
        url = args[0]; //"jdbc:postgresql://localhost/media?user=user";
        String dir = args[1];
        index(new File(dir));
    }

    private static void index(File rootDir) {
        //get the files in the folder
        File[] files = rootDir.listFiles();
        try {
            List<List<String>> records = new ArrayList<>();
            Stream.of(Objects.requireNonNull(files)).forEach(k -> {
                if (k.isDirectory()) {
                    if (!k.isHidden()) {
                        index(k.getAbsoluteFile());
                    }
                } else {
                    String fn = k.getName().toLowerCase();
                    if (fn.endsWith(".mp3") || fn.endsWith(".wav") || fn.endsWith(".flac") || fn.endsWith(".aac") || fn.endsWith(".aiff") ||
                            fn.endsWith(".wma") || fn.endsWith(".mp4")|| fn.endsWith(".m4a") ||fn.endsWith(".ogg")|| fn.endsWith(".dsf")) {
                        AudioFile af;
                        try {
                            af = AudioFileIO.read(k.getAbsoluteFile());
                            Tag tag = af.getTag();
                            records.add(Arrays.asList(tag.getFirst(FieldKey.TITLE), tag.getFirst(FieldKey.ALBUM), tag.getFirst(FieldKey.ARTIST), af.getFile().getAbsolutePath(), String.valueOf(af.getAudioHeader().getTrackLength())));
                        }
                        //maybe do individual things with the exceptions
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            });
            if(records.size() > 0) {
                saveRecords(records);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    /**
     * Saves the specified records to the media database
     * @param records List of List<String>, each nested List representing a row
     */
    private static void saveRecords(List<List<String>> records) {
        StringBuilder query = new StringBuilder();

        query.append("insert into media (title, artist, album, location, duration) values ");
        String data = String.join(",", records.stream().map(
                l -> { //L is a List<String> , returns ('field1','field2')
            return "(" +String.join(",", l.stream()
                    .map(s -> "'" + s.replace("'", "''") + "'")
                    .toArray(String[]::new)) + ")";
        }).toArray(String[]::new));

        query.append(data);
        try {
            int result = new DBAgent(url).bulkInsert(query.toString());
            System.out.println("" + result + " songs indexed");
        } catch (SQLException throwable) {
            System.out.println("indexing failed due to SQL error");
        }
    }
}
