Caelum Documentation
====================
The documents are generated using Sphinx restructured text, with the ReadTheDocs theme.

# Getting started

You need Docker to build Caelum documentation. Visit https://www.docker.com/ to get started with Docker.

1. Build sphinx+rtd theme docker image issuing command from the command line in current directory.
   on Linux:
   ```
   ./sphinx-rtd-theme-image.sh
   ```
   on Windows:
   ```
   sphinx-rtd-theme-image.bat
   ```

2. Run the Docker command with replacing /path/to/this/directory to actual path you have:
   ```
   docker run -it --rm -v /path/to/this/directory:/docs caelum/caelum-sphinx make clean html
   ```
   In case if you are on Windows and your Docker running under Docker Toolbox make a shared folder which
   is pointed to this directory and available for Docker VBox host. Then use the share by path inside the
   VBox host. For example if this directory is D:\work\caelum\docs then make share it with name /var/caelum_docs
   inside the host then run from your docker console
   ```
   docker run -it --rm -v /var/caelum_docs:/docs caelum/caelum-sphinx make clean html
   ```

3. If there were no errors documentation can be found in build subdirectory



