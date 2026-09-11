package cares.cam.ac.uk;

public class Config {
    public static final String DATABASE = System.getenv().getOrDefault("DATABASE", "postgres");
    public static final String NAMESPACE = System.getenv("NAMESPACE");
}
