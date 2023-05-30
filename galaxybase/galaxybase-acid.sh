#!/bin/bash
javac -nowarn -classpath graphdbapi-bolt-driver-3.5.0.jar Acid.java
java -classpath .:graphdbapi-bolt-driver-3.5.0.jar Acid
