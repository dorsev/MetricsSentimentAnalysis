enum SentimentStatus { Happy, Sad, Netural }
interface SentimentResult {
    sentiment: SentimentStatus
    confidence: number
}
interface SentimentAnalysis {
    calculateSentiment(str: String): SentimentResult
}

class SentimentAnalysisService implements SentimentAnalysis {
    calculateSentiment(str: String): SentimentResult {
        if (str.indexOf(":)") !== -1) {
            return { sentiment: SentimentStatus.Happy, confidence: 1 }
        } else if (str.indexOf(":(") !== -1) {
            return { sentiment: SentimentStatus.Sad, confidence: 1 }
        } else return { sentiment: SentimentStatus.Netural, confidence: 1 }
    }
}

interface KafkaProducer {
    produce(msg: string, topic: string, partition: number): boolean
}

let kafkaProducer: KafkaProducer = {
    produce(msg: string, topic: string, partition: number): boolean {
        console.info(`producing ${msg} to ${topic} in ${partition}`);
        return true;
    }
}

let senService: SentimentAnalysisService = new SentimentAnalysisService();

let tweetsInformation: string[] = ["I'm happy :) ", "I'm sad :("]
for (let index = 0; index < tweetsInformation.length; index++) {
    let tweetInformation = tweetsInformation[index]
    let sentimentResult = senService.calculateSentiment(tweetInformation)
    kafkaProducer.produce(JSON.stringify(sentimentResult), 'sentiment_topic', 0);
}
console.info("=====end=====");