Quick build:

`mvn clean && mvn package && mvn dependency:copy-dependencies -DincludeScope=compile && scp target/{dependency/*.jar,trippi-sail-0.0.1-SNAPSHOT.jar} vagrant@whip-vagrant-i7latest.local:/usr/local/fedora/tomcat/webapps/fedora/WEB-INF/lib`

Will want to make this a bit better... Dump everything into a ZIP or something.

... Will also have to nuke existing/older versions of the libraries... Or I guess we could drop them somewhere else with higher precedence, instead of inside Fedora's `WEB-INF/lib`.
