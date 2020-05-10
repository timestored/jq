
# jq - An implementation of q on the JVM

## About

Jq is an implementation of the [q language](https://en.wikipedia.org/wiki/Q_(programming_language_from_Kx_Systems)
using the JVM. It aims to be a complete, correct and fast implementation of q, in future it will provide powerful new features.

Currently it supports a subset of types, a small subset of operations and no on-disk storage. 
Visit the [jq website](https://www.timestored.com/jq) and the [jq wiki](https://github.com/timestored/jq/wiki) for more information.

## Getting Jq

You can  [download packages from the jq website](https://www.timestored.com/jq/download)  to either install or run in place. To run you will need a JRE (the Java VM runtime environment) version 8 or higher.
Alternatively, you can run a much slower version online at: (https://www.timestored.com/jq/online) (~1 minute to startup).


## Building Jq

```
 gradle clean build generateGrammarSource shadowJar
 java -jar jq\build\libs\jq-0.0.1-SNAPSHOT-all.jar
```

## Contributing

If you have experience or can help with:

 1. Automating the build within github to produce releases.
 2. Work with ubuntu/linux to all apt install of jq.
 3. Anything else.

Please [contact us](http://www.timestored.com/contact).


