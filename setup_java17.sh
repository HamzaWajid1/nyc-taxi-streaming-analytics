#!/bin/bash
# Script to configure Java 17 for PySpark

echo "Setting up Java 17 for PySpark..."

# Find Java 17 installation
JAVA17_PATH=$(update-alternatives --list java 2>/dev/null | grep "java-17" | head -1 | sed 's|/bin/java||')

if [ -z "$JAVA17_PATH" ]; then
    # Try to find it in common location
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        JAVA17_PATH="/usr/lib/jvm/java-17-openjdk-amd64"
    else
        echo "Error: Java 17 not found. Please install it first with:"
        echo "  sudo apt update && sudo apt install -y openjdk-17-jdk"
        exit 1
    fi
fi

echo "Found Java 17 at: $JAVA17_PATH"

# Set JAVA_HOME in current session
export JAVA_HOME=$JAVA17_PATH
export PATH=$JAVA_HOME/bin:$PATH

# Configure update-alternatives (requires sudo)
echo "Configuring Java 17 as default (requires sudo)..."
sudo update-alternatives --install /usr/bin/java java $JAVA17_PATH/bin/java 2
sudo update-alternatives --install /usr/bin/javac javac $JAVA17_PATH/bin/javac 2

# Set it as default
sudo update-alternatives --set java $JAVA17_PATH/bin/java
sudo update-alternatives --set javac $JAVA17_PATH/bin/javac

# Verify
echo ""
echo "Java version:"
java -version

echo ""
echo "JAVA_HOME: $JAVA_HOME"
echo ""
echo "Java 17 is now configured!"
echo "To make this permanent, add these lines to your ~/.bashrc or ~/.zshrc:"
echo "  export JAVA_HOME=$JAVA17_PATH"
echo "  export PATH=\$JAVA_HOME/bin:\$PATH"

