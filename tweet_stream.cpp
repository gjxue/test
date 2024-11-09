#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <random>
#include <chrono>
#include <fstream>
#include <sstream>
#include <sys/resource.h> 
#include <memory>
#include <set>
#include <queue>

#include <iostream>
#include <iomanip>
#include <ctime>
#include <chrono>
#include <string>

#include <iostream>
#include "json.hpp"
using json = nlohmann::json;


// Function to get the current timestamp as a formatted string
std::string currentTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t timeNow = std::chrono::system_clock::to_time_t(now);
    std::tm* tmNow = std::localtime(&timeNow);

    // Format the timestamp: [YYYY-MM-DD HH:MM:SS]
    std::stringstream ss;
    ss << "[" << std::put_time(tmNow, "%Y-%m-%d %H:%M:%S") << "]";
    return ss.str();
}

// Thread Pool class to manage filtering and normalization threads
class ThreadPool {
public:
    ThreadPool(size_t numThreads);
    ~ThreadPool();

    template<class F>
    void enqueue(F f);

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;
};

ThreadPool::ThreadPool(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i)
        workers.emplace_back([this] {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(this->queueMutex);
                    this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                    if (this->stop && this->tasks.empty())
                        return;
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                task();
            }
        });
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers)
        worker.join();
}

template<class F>
void ThreadPool::enqueue(F f) {
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        tasks.emplace(std::function<void()>(f));
    }
    condition.notify_one();
}

// Aho-Corasick Node for building the automaton
class AhoCorasickNode {
public:
    std::unordered_map<char, std::shared_ptr<AhoCorasickNode>> children;
    std::set<int> output;
    std::shared_ptr<AhoCorasickNode> fail;
};

// Aho-Corasick class
class AhoCorasick {
public:
    AhoCorasick(const std::vector<std::string>& keywords);
    std::vector<int> search(const std::string& text) const;

private:
    std::shared_ptr<AhoCorasickNode> root;
    void buildAutomaton();
};

AhoCorasick::AhoCorasick(const std::vector<std::string>& keywords) {
    root = std::make_shared<AhoCorasickNode>();
    // Build trie from keywords
    for (int i = 0; i < keywords.size(); ++i) {
        std::shared_ptr<AhoCorasickNode> node = root;
        for (char c : keywords[i]) {
            if (!node->children.count(c)) {
                node->children[c] = std::make_shared<AhoCorasickNode>();
            }
            node = node->children[c];
        }
        node->output.insert(i);
    }
    buildAutomaton();
}

void AhoCorasick::buildAutomaton() {
    std::queue<std::shared_ptr<AhoCorasickNode>> q;
    for (const auto& p : root->children) {
        p.second->fail = root;
        q.push(p.second);
    }

    while (!q.empty()) {
        std::shared_ptr<AhoCorasickNode> node = q.front();
        q.pop();

        for (const auto& p : node->children) {
            char ch = p.first;
            std::shared_ptr<AhoCorasickNode> child = p.second;
            std::shared_ptr<AhoCorasickNode> failNode = node->fail;

            while (failNode != nullptr && !failNode->children.count(ch)) {
                failNode = failNode->fail;
            }

            if (failNode) {
                child->fail = failNode->children[ch];
                child->output.insert(child->fail->output.begin(), child->fail->output.end());
            } else {
                child->fail = root;
            }
            q.push(child);
        }
    }
}

std::vector<int> AhoCorasick::search(const std::string& text) const {
    std::cout << currentTimestamp() << " Processing tweet" << std::endl;
    std::shared_ptr<AhoCorasickNode> node = root;
    std::vector<int> results;
    for (char c : text) {
        while (node != root && !node->children.count(c)) {
            node = node->fail;
        }
        if (node->children.count(c)) {
            node = node->children[c];
        }
        if (!node->output.empty()) {
            results.insert(results.end(), node->output.begin(), node->output.end());
        }
    }
    return results;
}

json convertTwitterV2ToActivityStream(const json& twitterV2Payload) {
    json activityStream = json::array();

    // Handle the case where "data" is a list
    if (twitterV2Payload.contains("data") && twitterV2Payload["data"].is_array()) {
        for (const auto& data : twitterV2Payload["data"]) {
            json activityItem;

            if (data.contains("created_at")) {
                activityItem["postedTime"] = data["created_at"];
                activityItem["object"]["postedTime"] = data["created_at"];
            }

            if (data.contains("source")) {
                activityItem["generator"]["displayName"] = data["source"];
            }

            if (data.contains("lang")) {
                activityItem["twitter_lang"] = data["lang"];
            }

            if (data.contains("text")) {
                activityItem["object"]["summary"] = data["text"];
            }

            if (data.contains("edit_history_tweet_ids")) {
                activityItem["object"]["edit_history"] = data["edit_history_tweet_ids"];
            }

            if (data.contains("edit_controls")) {
                activityItem["object"]["edit_controls"] = data["edit_controls"];
                if (data["edit_controls"].contains("is_edit_eligible")) {
                    activityItem["object"]["editable"] = data["edit_controls"]["is_edit_eligible"];
                }
            }

            if (data.contains("author_id")) {
                activityItem["actor"]["id"] = data["author_id"];
            }

            if (data.contains("in_reply_to_user_id")) {
                activityItem["inReplyTo"]["username"] = data["in_reply_to_user_id"];
            }

            if (data.contains("referenced_tweets")) {
                for (const auto& tweet : data["referenced_tweets"]) {
                    if (tweet.contains("id")) {
                        activityItem["inReplyTo"]["link"] = tweet["id"];
                    }
                    if (tweet.contains("type")) {
                        activityItem["inReplyTo"]["type"] = tweet["type"];
                    }
                }
            }

            if (data.contains("attachments")) {
                if (data["attachments"].contains("media_keys")) {
                    activityItem["twitter_entities"]["media"]["id_str"] = data["attachments"]["media_keys"];
                }
                if (data["attachments"].contains("poll_ids")) {
                    activityItem["twitter_entities"]["poll_ids"] = data["attachments"]["poll_ids"];
                }
            }

            if (data.contains("entities")) {
                activityItem["twitter_entities"] = json::object();
                if (data["entities"].contains("urls")) {
                    activityItem["twitter_entities"]["urls"] = json::array();
                    for (const auto& url : data["entities"]["urls"]) {
                        json urlObject;
                        urlObject["indices"] = { url.value("start", 0), url.value("end", 0) };
                        urlObject["url"] = url.value("url", "");
                        urlObject["expanded_url"] = url.value("expanded_url", "");
                        urlObject["display_url"] = url.value("display_url", "");
                        if (url.contains("status")) {
                            urlObject["status"] = url["status"];
                        }
                        if (url.contains("title")) {
                            urlObject["title"] = url["title"];
                        }
                        if (url.contains("description")) {
                            urlObject["description"] = url["description"];
                        }
                        if (url.contains("unwound_url")) {
                            urlObject["unwound_url"] = url["unwound_url"];
                        }
                        activityItem["twitter_entities"]["urls"].push_back(urlObject);
                    }
                }
                if (data["entities"].contains("cashtags")) {
                    activityItem["twitter_entities"]["symbols"] = json::array();
                    for (const auto& cashtag : data["entities"]["cashtags"]) {
                        json cashtagObject;
                        cashtagObject["indices"] = { cashtag.value("start", 0), cashtag.value("end", 0) };
                        cashtagObject["text"] = cashtag.value("tag", "");
                        activityItem["twitter_entities"]["symbols"].push_back(cashtagObject);
                    }
                }
                if (data["entities"].contains("hashtags")) {
                    activityItem["twitter_entities"]["hashtags"] = json::array();
                    for (const auto& hashtag : data["entities"]["hashtags"]) {
                        json hashtagObject;
                        hashtagObject["indices"] = { hashtag.value("start", 0), hashtag.value("end", 0) };
                        hashtagObject["text"] = hashtag.value("tag", "");
                        activityItem["twitter_entities"]["hashtags"].push_back(hashtagObject);
                    }
                }
                if (data["entities"].contains("mentions")) {
                    activityItem["twitter_entities"]["user_mentions"] = json::array();
                    for (const auto& mention : data["entities"]["mentions"]) {
                        json mentionObject;
                        mentionObject["indices"] = { mention.value("start", 0), mention.value("end", 0) };
                        mentionObject["screen_name"] = mention.value("tag", "");
                        mentionObject["id_str"] = mention.value("id", "");
                        activityItem["twitter_entities"]["user_mentions"].push_back(mentionObject);
                    }
                }
                if (data["entities"].contains("annotations")) {
                    activityItem["twitter_entities"]["annotations"] = json::array();
                    for (const auto& annotation : data["entities"]["annotations"] ) {
                        json annotationObject;
                        annotationObject["start"] = annotation.value("start", 0);
                        annotationObject["end"] = annotation.value("end", 0);
                        annotationObject["probability"] = annotation.value("probability", 0.0);
                        annotationObject["type"] = annotation.value("type", "");
                        annotationObject["normalized_text"] = annotation.value("normalized_text", "");
                        activityItem["twitter_entities"]["annotations"].push_back(annotationObject);
                    }
                }
            }

            if (data.contains("geo")) {
                activityItem["geo"] = data["geo"];
                if (data["geo"].contains("place_id")) {
                    activityItem["location"]["link"] = data["geo"]["place_id"];
                }
            }

            if (data.contains("public_metrics")) {
                if (data["public_metrics"].contains("like_count")) {
                    activityItem["favoritesCount"] = data["public_metrics"]["like_count"];
                }
                if (data["public_metrics"].contains("retweet_count")) {
                    activityItem["retweetCount"] = data["public_metrics"]["retweet_count"];
                }
                if (data["public_metrics"].contains("quote_count")) {
                    activityItem["quoteCount"] = data["public_metrics"]["quote_count"];
                }
                if (data["public_metrics"].contains("reply_count")) {
                    activityItem["replyCount"] = data["public_metrics"]["reply_count"];
                }
            }

            activityStream.push_back(activityItem);
        }
    }

    if (twitterV2Payload.contains("includes")) {
        const json& includes = twitterV2Payload["includes"];

        if (includes.contains("users")) {
            for (const auto& user : includes["users"]) {
                json actor;
                if (user.contains("id")) {
                    actor["id"] = user["id"];
                }
                if (user.contains("name")) {
                    actor["displayName"] = user["name"];
                }
                if (user.contains("username")) {
                    actor["preferredUsername"] = user["username"];
                }
                if (user.contains("created_at")) {
                    actor["postedTime"] = user["created_at"];
                }
                if (user.contains("description")) {
                    actor["summary"] = user["description"];
                }
                if (user.contains("public_metrics")) {
                    if (user["public_metrics"].contains("followers_count")) {
                        actor["followersCount"] = user["public_metrics"]["followers_count"];
                    }
                    if (user["public_metrics"].contains("following_count")) {
                        actor["friendsCount"] = user["public_metrics"]["following_count"];
                    }
                    if (user["public_metrics"].contains("listed_count")) {
                        actor["listedCount"] = user["public_metrics"]["listed_count"];
                    }
                    if (user["public_metrics"].contains("tweet_count")) {
                        actor["statusesCount"] = user["public_metrics"]["tweet_count"];
                    }
                }
                if (user.contains("location")) {
                    actor["location"]["displayName"] = user["location"];
                }
                if (user.contains("profile_image_url")) {
                    actor["image"] = user["profile_image_url"];
                }
                if (user.contains("url")) {
                    actor["links"] = json::array();
                    actor["links"].push_back(user["url"]);
                }
                if (user.contains("verified")) {
                    actor["verified"] = user["verified"];
                }
                if (user.contains("entities") && user["entities"].contains("url") && user["entities"]["url"].contains("urls")) {
                    for (const auto& url : user["entities"]["url"]["urls"]) {
                        json linkObject;
                        linkObject["expanded_url"] = url.value("expanded_url", "");
                        actor["links"].push_back(linkObject);
                    }
                }
                activityStream.push_back({{"actor", actor}});
            }
        }

        if (includes.contains("media")) {
            for (const auto& media : includes["media"]) {
                json mediaObject;
                if (media.contains("media_key")) {
                    mediaObject["id_str"] = media["media_key"];
                }
                if (media.contains("preview_image_url")) {
                    mediaObject["media_url_https"] = media["preview_image_url"];
                }
                if (media.contains("type")) {
                    mediaObject["type"] = media["type"];
                }
                if (media.contains("height")) {
                    mediaObject["sizes"]["large"]["h"] = media["height"];
                }
                if (media.contains("width")) {
                    mediaObject["sizes"]["large"]["w"] = media["width"];
                }
                if (media.contains("duration_ms")) {
                    mediaObject["video_info"]["duration_millis"] = media["duration_ms"];
                }
                if (media.contains("alt_text")) {
                    mediaObject["alt_text"] = media["alt_text"];
                }
                activityStream.push_back({{"media", mediaObject}});
            }
        }

        if (includes.contains("places")) {
            for (const auto& place : includes["places"]) {
                json locationObject;
                if (place.contains("full_name")) {
                    locationObject["displayName"] = place["full_name"];
                }
                if (place.contains("id")) {
                    locationObject["link"] = place["id"];
                }
                if (place.contains("name")) {
                    locationObject["name"] = place["name"];
                }
                if (place.contains("country")) {
                    locationObject["country_code"] = place["country"];
                }
                if (place.contains("place_type")) {
                    locationObject["twitter_place_type"] = place["place_type"];
                }
                if (place.contains("country_code")) {
                    locationObject["twitter_country_code"] = place["country_code"];
                }
                if (place.contains("geo")) {
                    if (place["geo"].contains("type")) {
                        locationObject["geo"]["type"] = place["geo"]["type"];
                    }
                    if (place["geo"].contains("bbox")) {
                        locationObject["geo"]["coordinates"] = place["geo"]["bbox"];
                    }
                }
                activityStream.push_back({{"location", locationObject}});
            }
        }
    }

    return activityStream;
}

// Function to normalize tweets (to lowercase and remove special characters)
std::string normalizeTweet(const std::string& tweet) {
    // Parse the input string into JSON
    json twitterV2Payload = json::parse(tweet);

    // Convert the Twitter V2 payload to Activity Stream format
    json activityStream = convertTwitterV2ToActivityStream(twitterV2Payload);

    // Convert the output JSON to a string and return
    return activityStream.dump(4); // Pretty-print with an indent of 4 spaces
}

// Example function to simulate processing incoming tweets
void processTweet(const std::string& tweet, const AhoCorasick& ac) {
    std::vector<int> matches = ac.search(tweet);
    int randomValue = std::rand() % 50;
    if (!matches.empty() && randomValue == 0) {
        std::string normalized = normalizeTweet(tweet);
        // Output or further process the filtered and normalized tweet
        std::cout << currentTimestamp() << " Normalized tweet: " << normalized << std::endl;
    }
    else {
	std::cout << currentTimestamp() << " Filtered out" << std::endl;
    }
}

// Function to generate random tweets for simulation
std::string generateRandomTweet() {
    // Sample randomized text options for the tweet content
    const std::vector<std::string> sample_texts = {
        "Check out the latest news in tech! #Innovation",
        "Excited to announce our new project launching soon! ",
        "We're expanding our community to better serve everyone! ",
        "Exploring the latest in #AI and #MachineLearning.",
        "Stay tuned for updates on our upcoming features! ",
        "Just reached a new milestone! Thanks to all our supporters! "
    };

    // Select a random text for the tweet (limited to 120 characters)
    std::string random_text = sample_texts[rand() % sample_texts.size()].substr(0, 120);

    // Construct JSON output
    std::string json = R"({
      "data": [
        {
          "lang": "en",
          "conversation_id": "1293593516040269825",
          "text": ")" + random_text + R"(",
          "attachments": {
            "media_keys": [
              "7_1293565706408038401"
            ]
          },
          "possibly_sensitive": false,
          "entities": {
            "annotations": [
              {
                "start": 78,
                "end": 88,
                "probability": 0.4381,
                "type": "Product",
                "normalized_text": "Twitter API"
              }
            ],
            "hashtags": [
              {
                "start": 42,
                "end": 53,
                "tag": "TwitterAPI"
              }
            ],
            "urls": [
              {
                "start": 195,
                "end": 218,
                "url": "https://t.co/32VrwpGaJw",
                "expanded_url": "https://blog.twitter.com/developer/en_us/topics/tools/2020/introducing_new_twitter_api.html",
                "display_url": "blog.twitter.com/developer/en_u…",
                "images": [
                  {
                    "url": "https://pbs.twimg.com/news_img/1336475659279818754/_cmRh7QE?format=jpg&name=orig",
                    "width": 1200,
                    "height": 627
                  },
                  {
                    "url": "https://pbs.twimg.com/news_img/1336475659279818754/_cmRh7QE?format=jpg&name=150x150",
                    "width": 150,
                    "height": 150
                  }
                ],
                "status": 200,
                "title": "Introducing a new and improved Twitter API",
                "description": "Introducing the new Twitter API - rebuilt from the ground up to deliver new features faster so developers can help the world connect to the public conversation happening on Twitter.",
                "unwound_url": "https://blog.twitter.com/developer/en_us/topics/tools/2020/introducing_new_twitter_api.html"
              },
              {
                "start": 219,
                "end": 242,
                "url": "https://t.co/KaFSbjWUA8",
                "expanded_url": "https://twitter.com/TwitterDev/status/1293593516040269825/video/1",
                "display_url": "pic.twitter.com/KaFSbjWUA8"
              }
            ]
          },
          "id": "1293593516040269825",
          "public_metrics": {
            "retweet_count": 958,
            "reply_count": 171,
            "like_count": 2848,
            "quote_count": 333
          },
          "author_id": "2244994945",
          "context_annotations": [
            {
              "domain": {
                "id": "46",
                "name": "Brand Category",
                "description": "Categories within Brand Verticals that narrow down the scope of Brands"
              },
              "entity": {
                "id": "781974596752842752",
                "name": "Services"
              }
            },
            {
              "domain": {
                "id": "47",
                "name": "Brand",
                "description": "Brands and Companies"
              },
              "entity": {
                "id": "10045225402",
                "name": "Twitter"
              }
            },
            {
              "domain": {
                "id": "65",
                "name": "Interests and Hobbies Vertical",
                "description": "Top level interests and hobbies groupings, like Food or Travel"
              },
              "entity": {
                "id": "848920371311001600",
                "name": "Technology",
                "description": "Technology and computing"
              }
            },
            {
              "domain": {
                "id": "66",
                "name": "Interests and Hobbies Category",
                "description": "A grouping of interests and hobbies entities, like Novelty Food or Destinations"
              },
              "entity": {
                "id": "848921413196984320",
                "name": "Computer programming",
                "description": "Computer programming"
              }
            }
          ],
          "source": "Twitter Web App",
          "created_at": "2020-08-12T17:01:42.000Z"
        }
      ],
      "includes": {
        "media": [
          {
            "height": 720,
            "duration_ms": 34875,
            "media_key": "7_1293565706408038401",
            "type": "video",
            "preview_image_url": "https://pbs.twimg.com/ext_tw_video_thumb/1293565706408038401/pu/img/66P2dvbU4a02jYbV.jpg",
            "public_metrics": {
              "view_count": 279438
            },
            "width": 1280
          }
        ],
        "users": [
          {
            "created_at": "2013-12-14T04:35:55.000Z",
            "id": "2244994945",
            "protected": false,
            "username": "TwitterDev",
            "verified": true,
            "entities": {
              "url": {
                "urls": [
                  {
                    "start": 0,
                    "end": 23,
                    "url": "https://t.co/3ZX3TNiZCY",
                    "expanded_url": "https://developer.twitter.com/en/community",
                    "display_url": "developer.twitter.com/en/community"
                  }
                ]
              },
              "description": {
                "hashtags": [
                  {
                    "start": 17,
                    "end": 28,
                    "tag": "TwitterDev"
                  },
                  {
                    "start": 105,
                    "end": 116,
                    "tag": "TwitterAPI"
                  }
                ]
              }
            },
            "description": "The voice of the #TwitterDev team and your official source for updates, news, and events, related to the #TwitterAPI.",
            "pinned_tweet_id": "1293593516040269825",
            "public_metrics": {
              "followers_count": 513962,
              "following_count": 2039,
              "tweet_count": 3635,
              "listed_count": 1672
            },
            "location": "127.0.0.1",
            "name": "Twitter Dev",
            "profile_image_url": "https://pbs.twimg.com/profile_images/1283786620521652229/lEODkLTh_normal.jpg",
            "url": "https://t.co/3ZX3TNiZCY"
          }
        ]
      }
    })";

    return json;
}

// Function to load keywords from a file (one keyword per line)
std::vector<std::string> loadKeywordsFromFile(const std::string& filename) {
    std::ifstream file(filename);
    std::vector<std::string> keywords;
    std::string line;
    
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return keywords;
    }
    
    while (std::getline(file, line)) {
        if (!line.empty()) {
            keywords.push_back(line);
        }
    }
    return keywords;
}

// Main function to simulate fetching tweets from a Firehose API
void streamTweets(AhoCorasick& ac, ThreadPool& pool) {
    const int tweetsPerSecond = 5000;
    const std::chrono::milliseconds interval(1000 / tweetsPerSecond); // Delay per tweet to simulate 5000 tweets/sec

    auto start = std::chrono::steady_clock::now();
    int tweetsSent = 0;

    while (true) {
        pool.enqueue([&ac]() {
            std::string tweet = generateRandomTweet();
            processTweet(tweet, ac);
        });

        // Sleep to maintain the rate of 5000 tweets per second
        std::this_thread::sleep_until(start + tweetsSent * interval);
        tweetsSent++;
    }
}

int main() {
    // Load keywords from a file
    std::vector<std::string> keywords = loadKeywordsFromFile("keywords.txt");
    if (keywords.empty()) {
        std::cerr << "No keywords loaded!" << std::endl;
        return 1;
    }

    // Initialize the Aho-Corasick automaton with the loaded keywords
    AhoCorasick ac(keywords);

    // Initialize thread pool with 4 threads
    ThreadPool pool(6);

    // Simulate streaming tweets and processing them at 5000 tweets per second
    streamTweets(ac, pool);

    return 0;
}

