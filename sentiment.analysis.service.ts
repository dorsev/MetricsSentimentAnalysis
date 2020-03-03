import SDC = require("statsd-client");

let sdc = new SDC({ host: 'localhost' });


enum SentimentStatus { Happy, Sad, Netural }
interface SentimentResult {
    sentiment: SentimentStatus[]
    confidence: number
}
interface SentimentAnalysis {
    calculateSentiment(str: String): SentimentResult | undefined
}
interface KafkaConsumer {
    consume(): KafkaMessage
}
interface KafkaMessage {
    msg: string,
    offset: number
}
interface KafkaProducer {
    produce(msg: string, topic: string, partition: number): boolean
}
interface SentimentStore {
    store(sentimentResult: SentimentResult): boolean
}

class SentimentAnalysisService implements SentimentAnalysis {
    calculateSentiment(str: String): SentimentResult | undefined {
        var result: SentimentResult;
        try {
            const startTime = +new Date();
            if (str.indexOf(":)") !== -1) {
                result = { sentiment: [SentimentStatus.Happy], confidence: 1 }
            } else if (str.indexOf(":(") !== -1) {
                result = { sentiment: [SentimentStatus.Sad], confidence: 1 }
            } else result = { sentiment: [SentimentStatus.Netural], confidence: 1 }
            sdc.timing('processing_duration_ms', startTime);
            sdc.counter('sentiment_score_detected', result.sentiment[0], { 'sentiment': result.sentiment[0] });
            sdc.gauge('sentiment_score_per_tweet', result.sentiment[0], { 'sentiment': result.sentiment[0] });
            sdc.gauge('sentiment_values_length', result.sentiment.length);
            return result;
        } catch (e) {
            sdc.increment('failed_requests_count');
            return undefined;
        }
    }
}

let kafkaProducer: KafkaProducer = {
    produce(msg: string, topic: string, partition: number): boolean {
        sdc.increment('outgoing_events_count');
        console.info(`producing ${msg} to ${topic} in ${partition}`);
        return true;
    }
}

let sentimentStore: SentimentStore = {
    store(sentimentResult: SentimentResult): boolean {
        const startTime = +new Date();
        console.info(`storing ${JSON.stringify(sentimentResult)} to db`);
        sdc.timing('store_duration_ms', startTime);
        return true;
    }
}

var count: number = 1

let kafkaConsumer: KafkaConsumer = {
    consume(): KafkaMessage {
        if (count % 2 == 0) {
            count++;
            return {
                msg: `{"msg": "I'm sad ${count} :("}`,
                offset: count
            }
        } else {
            count++;
            return {
                msg: `{"msg": "I'm happy ${count} :)"}`,
                offset: count
            }
        }
    }
}

function deSerialize(str: string): any {
    const startTime = +new Date();
    const parsedResult = JSON.parse(str);
    sdc.timing('deserialize_duration_ms', startTime);
    return parsedResult;
}

function serialize(str: any): string {
    const startTime = +new Date();
    const serializedResult = JSON.stringify(str);
    sdc.timing('serialize_duration_ms', startTime);
    return serializedResult;
}

let senService: SentimentAnalysisService = new SentimentAnalysisService();

const partition: number = 0
while (true) {
    let tweetInformation = kafkaConsumer.consume()
    sdc.increment('incoming_requests_count')
    sdc.gauge('incoming_offset', tweetInformation.offset, { partition_num: partition });
    let deserializedTweet: { msg: string } = deSerialize(tweetInformation.msg)
    sdc.histogram('request_size_chars', deserializedTweet.msg.length);
    let sentimentResult = senService.calculateSentiment(deserializedTweet.msg)
    if (sentimentResult !== undefined) {
        let seriarliedSentimentResult = serialize(sentimentResult)
        sdc.histogram('outgoing_event_size_chars', seriarliedSentimentResult.length);
        sentimentStore.store(sentimentResult)
        kafkaProducer.produce(seriarliedSentimentResult, 'sentiment_topic', partition);
    }

}