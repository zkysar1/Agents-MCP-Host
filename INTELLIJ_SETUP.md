# IntelliJ IDEA Setup Guide for Agents-MCP-Host

## Configuration Complete! ✅

I've updated your Gradle configuration to work properly with IntelliJ's run button. Here's what was changed and how to use it:

## Changes Made

### 1. Enhanced build.gradle.kts
- Added explicit `run` task configuration with main class
- Configured JVM arguments for Windows compatibility
- Added dependency on `classes` task to ensure compilation
- Fixed the deprecated `buildDir` reference

### 2. Created gradle.properties
- Set JVM memory settings (2GB heap)
- Enabled Gradle daemon for faster builds
- Added Windows-specific optimizations
- Configured file encoding and headless mode

## How to Use IntelliJ's Run Button

### Method 1: Gradle Run Configuration (Recommended)
1. Open IntelliJ IDEA
2. On the right side, open the **Gradle** panel
3. Navigate to: `Agents-MCP-Host → Tasks → application → run`
4. Right-click `run` and select **Run 'Agents-MCP-Host [run]'**
5. IntelliJ will create a run configuration you can use with the green run button

### Method 2: Create Application Configuration
1. Click **Run → Edit Configurations**
2. Click the **+** button → **Application**
3. Configure:
   - **Name**: Agents-MCP-Host
   - **Main class**: AgentsMCPHost.Driver
   - **Use classpath of module**: Agents-MCP-Host.main
   - **Working directory**: $ProjectFileDir$
4. Click **OK**
5. Now you can use the green run button from any file

### Method 3: Let IntelliJ Auto-Configure
1. **File → Settings** (or **IntelliJ IDEA → Preferences** on Mac)
2. Navigate to **Build, Execution, Deployment → Build Tools → Gradle**
3. Set **Build and run using**: Gradle
4. Set **Run tests using**: Gradle
5. Click **OK**
6. IntelliJ will now use Gradle for all operations

## Fixing File Lock Issues

If you still encounter file lock issues:

### Quick Fix:
```bash
# In IntelliJ Terminal (Alt+F12):
./gradlew --stop     # Stops all Gradle daemons
./gradlew clean      # Cleans build directory
./gradlew run        # Runs the application
```

### Permanent Fix:
The new configuration includes:
- `dependsOn("classes")` - ensures proper build order
- Windows-optimized JVM settings
- Gradle daemon for better performance
- File lock handling in clean task

## Running Without Driver.java Open

You can now:
1. Press **Shift+F10** (Run) from any file
2. Click the green arrow in the toolbar
3. Use **Run → Run 'Agents-MCP-Host'** from menu
4. Double-click the `run` task in Gradle panel

## Troubleshooting

### If Run Button Still Doesn't Work:
1. **Invalidate Caches**: File → Invalidate Caches and Restart
2. **Reimport Project**: Right-click on build.gradle.kts → Reimport Gradle Project
3. **Check SDK**: File → Project Structure → Project → Ensure Java 21 is selected

### If File Locks Persist:
1. Close IntelliJ completely
2. Open Task Manager
3. End all `java.exe` processes
4. Delete the `build` folder manually
5. Restart IntelliJ

## Verification

To verify everything works:
```bash
./gradlew clean compileJava
./gradlew run
```

Both commands should complete successfully without file lock errors.

## Benefits of This Configuration

- ✅ Run button works from any file
- ✅ Reduced file lock issues on Windows
- ✅ Faster builds with Gradle daemon
- ✅ Proper memory allocation (2GB)
- ✅ Consistent encoding (UTF-8)
- ✅ IntelliJ properly recognizes the project structure