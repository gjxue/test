#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <ctime>

std::string generateRandomKeyword(int length) {
    const std::string characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string keyword;
    for (int i = 0; i < length; ++i) {
        keyword += characters[rand() % characters.size()];
    }
    return keyword;
}

int main() {
    // Seed for random generator
    srand(static_cast<unsigned int>(time(0)));

    // Open a file to store the keywords
    std::ofstream outputFile("keywords.txt");
    if (!outputFile) {
        std::cerr << "Error opening file!" << std::endl;
        return 1;
    }

    const int numKeywords = 200000;
    const int minKeywordLength = 3;
    const int maxKeywordLength = 15;

    for (int i = 0; i < numKeywords; ++i) {
        int length = minKeywordLength + rand() % (maxKeywordLength - minKeywordLength + 1);
        std::string keyword = generateRandomKeyword(length);
        outputFile << keyword << "\n";
    }

    outputFile.close();
    std::cout << "200,000 random tweet keywords generated in 'keywords.txt'." << std::endl;

    return 0;
}

