#include "exerciser.h"
#include "query_funcs.h"

void exercise(connection *C) {
    cout << "-------------Query1-------------\n";
    query1(C, 1, 35, 40,
              0, 15, 18,
              0, 0, 0,
              0, 0, 0,
              0, 0, 0,
              0, 0, 0);
    cout << "-------------Query2-------------\n";
    query2(C, "Orange");
    cout << "-------------Query3-------------\n";
    query3(C, "Duke");
    cout << "-------------Query4-------------\n";
    query4(C, "NC", "DarkBlue");
    cout << "-------------Query5-------------\n";
    query5(C, 6);
}
