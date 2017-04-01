package fr.devoxx.kafka.pojo;

/**
 * Created by sdumas on 18/03/17.
 */
public class GitMessage {

    private String hash;
    private String date;

    private String author;
    private String message;

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String toString() {
        return hash + " " + date + " " + author + " " + message;
    }
}
