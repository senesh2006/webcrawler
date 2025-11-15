#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>

int main() {
    sqlite3 *db;
    sqlite3_stmt *res;
    
    // 1. Open the database file (Use forward slashes!)
    // REPLACE THIS PATH with the actual path to your crawler.db
    int rc = sqlite3_open("crawler.db", &db);
    
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // 2. Prepare the SQL query
    // We are selecting the URL and Word Count from the 'pages' table
    char *sql = "SELECT url, word_count FROM pages WHERE status_code = 200 LIMIT 5;";

    rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
    
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to fetch data: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    printf("--- TOP 5 CRAWLED PAGES ---\n");

    // 3. Loop through the results
    while (sqlite3_step(res) == SQLITE_ROW) {
        // Column 0 is 'url' (Text)
        const unsigned char *url = sqlite3_column_text(res, 0);
        // Column 1 is 'word_count' (Integer)
        int word_count = sqlite3_column_int(res, 1);

        printf("Words: %d | URL: %s\n", word_count, url);
    }

    // 4. Cleanup
    sqlite3_finalize(res);
    sqlite3_close(db);
    
    return 0;
}