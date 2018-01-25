![recorder-image](https://user-images.githubusercontent.com/3109377/35347322-bab22268-012c-11e8-80d8-ef29d637fed7.jpeg)

# scoop

/skuːp/ informal

*a piece of news published by a newspaper or broadcast by a television or radio station in advance of its rivals.*

The original goal of this project was to talk to *Apache Kafka* directly, without any middleman (e.g. client/admin API) and take advantage of the Clojure REPL as a conversational interface. Removing the API from the equation means  that we can speak Kafka's mother tongue (Kafka protocol) without the API censoring us (e.g. validating what we send or restricting which information we get). In a nutshell, to make an interview to Kafka from the REPL. 

An evolution over this idea is to use *clojure.spec* to describe Kafka's protocol requests and combine it with some generators from *clojure.test.check* to see how Kafka deals with malformed inputs at the protocol level. By adding some crash awareness this tool can easily become a Kafka protocol fuzzer.

## Usage

Just spin up a REPL and use BOOTSTRAP_SERVERS environment var to point to a Kafka cluster. 

```
BOOTSTRAP_SERVERS="kafka:9092" lein repl
```

![image](https://user-images.githubusercontent.com/3109377/35306289-d0b92158-0094-11e8-9a84-3edd99e8e3e3.png)
## Resources

This tool was part of the presentation [Conversations with Kafka](https://github.com/nachomdo/scoop/files/1661729/Conversation.with.Kafka.pdf) for the [London Clojurians group](https://www.meetup.com/London-Clojurians/)