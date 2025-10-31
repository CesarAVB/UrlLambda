package br.com.rise.model;

public class UrlData {

    private String originalUrl;
    private long expirationTime;

    public UrlData() {
    }

    public UrlData(String originalUrl, long expirationTime) {
        this.originalUrl = originalUrl;
        this.expirationTime = expirationTime;
    }

    public String getOriginalUrl() {
        return originalUrl;
    }

    public void setOriginalUrl(String originalUrl) {
        this.originalUrl = originalUrl;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }
}
