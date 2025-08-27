#!/bin/bash

# Fix broken method calls in logger statements
# Pattern: method( + " should be method() + "

echo "Fixing broken method calls in logger statements..."

# Find and fix all occurrences
find src/main/java/agents/director -name "*.java" -type f | while read file; do
    # Fix patterns like .size( + " to .size() + "
    sed -i 's/\.size( + "/\.size() + "/g' "$file"
    
    # Fix patterns like .cause( + " to .cause() + "
    sed -i 's/\.cause( + "/\.cause() + "/g' "$file"
    
    # Fix patterns like .getName( + " to .getName() + "
    sed -i 's/\.getName( + "/\.getName() + "/g' "$file"
    
    # Fix patterns like .encode( + " to .encode() + "
    sed -i 's/\.encode( + "/\.encode() + "/g' "$file"
    
    # Fix patterns like .toString( + " to .toString() + "
    sed -i 's/\.toString( + "/\.toString() + "/g' "$file"
    
    # Fix patterns like .getMessage( + " to .getMessage() + "
    sed -i 's/\.getMessage( + "/\.getMessage() + "/g' "$file"
done

echo "Fix completed!"