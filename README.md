# Trippi-Sail Triplestore Adapter

## Introduction

This module allows Sesame Sail-compliant triplestores to be plugged into the Trippi triplestore interface, ultimately allowing them to be used with the Fedora Commons Repository.

## Requirements

This module requires the following:

* [Tomcat >= 7.0](https://tomcat.apache.org)
* [A Sesame Sail compliant Triplestore](http://rdf4j.org/sesame/2.7/docs/users.docbook?view#The_Repository_API)

## Compilation

The code should be able to be built with Maven 3, simply cloning the repository, navigating into the cloned repository and calling:

```
mvn package
```

This should generate both a `zip` and `tar.gz` file inside of the `target` directory.

## Installation

1. Install your Sail-compliant triplestore. NOTE: Do not put it in the same servlet container as Fedora, since Fedora cannot be running when rebuilding the RI.
1. Extract the archive onto your target machine. Let's assume we have extracted it into `/opt/trippi-sail`.
1. Configure Fedora to know of the extracted JARs, in `$CATALINA_BASE/conf/Catalina/localhost/fedora.xml`:

    ```xml
    <Context>
    	<Loader
    		className="org.apache.catalina.loader.VirtualWebappLoader"
    		virtualClasspath="/opt/trippi-sail/*.jar"
    		searchVirtualFirst="true"/>
    	[...]
    </Context>
    ```

    NOTE: It may be desirable to add other paths to the `virtualClasspath` value if you are using a triplestore other than Blazegraph (1.5.3), in order to make the relevant classes available for the next step.
1. Use Spring Bean configuration to inject a `org.openrdf.repository.Repository` implementation into our `org.trippi.impl.sesame.SesameSession` class. See the examples in the `src/main/resources/sample-bean-config-xml` directory from the source or `/opt/trippi-sail/example-bean-xml` directory from the extracted binary package for examples. The XML file should be created in `$FEDORA_HOME/server/config/spring`.
1. Remove the reference to the resource index datastore in `$FEDORA_HOME/server/config/fedora.fcfg`; commenting it out should suffice. The section in particular is something like:

    ```xml
    <server xmlns="http://www.fedora.info/definitions/1/0/config/" class="org.fcrepo.server.BasicServer">
        [...]
        <module role="org.fcrepo.server.resourceIndex.ResourceIndex" class="org.fcrepo.server.resourceIndex.ResourceIndexModule">
            [...]
            <!-- Remove/comment out following param[@name="datastore"] bit in this module. -->
            <param name="datastore" value="localMulgaraTriplestore">
                <comment>(required)
                    Name of the triplestore to use. WARNING: changing the
                    triplestore running the Resource Index Rebuilder.</comment>
            </param>
            [...]
        </module>
        [...]
    </server>
    ```
    
1. The RI rebuild does not use the same classloader used by Tomcat, so the above `virtualClasspath` is not sufficient. In order to be able to run the rebuild, the classpath will have to be added to the rebuild command. The easiest way to do this is to prepend to the `-cp` entry in `$FEDORA_HOME/server/bin/env-server.sh`; enter the path to the directory, followed by an asterisk, like: `/opt/trippi-sail/*`.
1. Trippi as embedded in Fedora itself has Sesame/OpenRDF jars built-in, which cause NullPointerExceptions when querying, so it is necessary to move/rename `$CATALINA_BASE/webapps/fedora/WEB-INF/lib/openrdf-sesame-onejar-{version}.jar` such that it is not eligible for the classloader (change the extension away from `jar` or delete the file entirely, or move it somewhere else)..
1. Now, we should be clear to stop Fedora, rebuild the RI and start Fedora, and that should be it: You should now be running Fedora with your other triplestore backing the RI.

## Troubleshooting/Issues

Having problems or solved a problem? Contact [discoverygarden](http://support.discoverygarden.ca).

## Maintainers/Sponsors

Current maintainers:

* [discoverygarden](http://www.discoverygarden.ca)

## Development

If you would like to contribute to this module, please check out our helpful
[Documentation for Developers](https://github.com/Islandora/islandora/wiki#wiki-documentation-for-developers)
info, [Developers](http://islandora.ca/developers) section on Islandora.ca and
contact [discoverygarden](http://support.discoverygarden.ca).
