#include "query_funcs.h"
#include <cstdlib>
#include <sstream>
#include <fstream>

#define stateFile "state.txt"
#define colorFile "color.txt"
#define teamFile "team.txt"
#define playerFile "player.txt"

// escape "'" in string
string sanitizeString(const string &orig) {
    string after;
    for (char c : orig) {
        if (c == '\'') {
            after.push_back('\'');
        }
        after.push_back(c);
    }
    return after;
}

void dropATable(connection *C, string tableName) {
    /* Create a transactional object. */
    work W(*C);

    string sql = "DROP TABLE ";
    sql += tableName;
    sql += ";";

    /* Execute SQL query */
    try {
        W.exec(sql);
        W.commit();
        cout << "Table " << tableName << " dropped successfully" << endl;
    }
    catch (const undefined_table &e) {
        cout << "Table " << tableName << " doesn't exist. Ignoring the drop.\n";
    }
    catch (const exception &e) {
        cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}

// drop all the existing tables
void cleanTables(connection *C) {
    dropATable(C, "player");
    dropATable(C, "team");
    dropATable(C, "state");
    dropATable(C, "color");
}

// TODO: create PLAYER, TEAM, STATE, and COLOR tables in the ACC_BBALL database
//       load each table with rows from the provided source txt files
void createTables(connection *C) {
    /* Create a transactional object. */
    work W(*C);

    /* Create SQL statement */
    string createState = "CREATE TABLE STATE (\
    STATE_ID      INT        NOT NULL,\
    NAME          CHAR(2)    NOT NULL,\
    CONSTRAINT STATEID_PK PRIMARY KEY (STATE_ID)\
    )";
    string createColor = "CREATE TABLE COLOR (\
    COLOR_ID      INT            NOT NULL,\
    NAME          VARCHAR(20)    NOT NULL,\
    CONSTRAINT COLORID_PK PRIMARY KEY (COLOR_ID)\
    )";
    string createTeam = "CREATE TABLE TEAM (\
    TEAM_ID       INT            NOT NULL,\
    NAME          VARCHAR(25)    NOT NULL,\
    STATE_ID      INT            NOT NULL,\
    COLOR_ID      INT            NOT NULL,\
    WINS          INT            NOT NULL,\
    LOSSES        INT            NOT NULL,\
    CONSTRAINT TEAMID_PK PRIMARY KEY (TEAM_ID),\
    CONSTRAINT STATEIDFK FOREIGN KEY (STATE_ID) REFERENCES STATE(STATE_ID) ON DELETE SET NULL ON UPDATE CASCADE,\
    CONSTRAINT COLORIDNFK FOREIGN KEY (COLOR_ID) REFERENCES COLOR(COLOR_ID) ON DELETE SET NULL ON UPDATE CASCADE\
    )";
    string createPlayer = "CREATE TABLE PLAYER (\
    PLAYER_ID     INT            NOT NULL,\
    TEAM_ID       INT            NOT NULL,\
    UNIFORM_NUM   INT            NOT NULL,\
    FIRST_NAME    VARCHAR(20)    NOT NULL,\
    LAST_NAME     VARCHAR(20)    NOT NULL,\
    MPG           INT            NOT NULL,\
    PPG           INT            NOT NULL,\
    RPG           INT            NOT NULL,\
    APG           INT            NOT NULL,\
    SPG           DECIMAL(2,1)   NOT NULL,\
    BPG           DECIMAL(2,1)   NOT NULL,\
    CONSTRAINT PLAYERID_PK PRIMARY KEY (PLAYER_ID),\
    CONSTRAINT TEAMIDFK FOREIGN KEY (TEAM_ID) REFERENCES TEAM(TEAM_ID) ON DELETE SET NULL ON UPDATE CASCADE\
    )";

    /* Execute SQL query */
    try {
        W.exec(createState);
        W.exec(createColor);
        W.exec(createTeam);
        W.exec(createPlayer);
        W.commit();
        cout << "All tables created successfully" << endl;
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}

void fillState(connection *C) {
    ifstream inFile(stateFile);

    string line;
    while (getline(inFile, line)) {
        stringstream ss(line);
        int id;
        string name;
        if (!(ss >> id >> name))
            break;

        add_state(C, name);
    }

    inFile.close();
}

void fillColor(connection *C) {
    ifstream inFile(colorFile);

    string line;
    while (getline(inFile, line)) {
        stringstream ss(line);
        int id;
        string name;
        if (!(ss >> id >> name))
            break;

        add_color(C, name);
    }

    inFile.close();
}

void fillTeam(connection *C) {
    ifstream inFile(teamFile);

    // 1 BostonCollege 10 7 2 16
    string line;
    while (getline(inFile, line)) {
        stringstream ss(line);
        int teamId;
        string name;
        int stateId;
        int colorId;
        int wins;
        int losses;
        if (!(ss >> teamId >> name >> stateId >> colorId >> wins >> losses))
            break;

        add_team(C, name, stateId, colorId, wins, losses);
    }

    inFile.close();
}

void fillPlayer(connection *C) {
    ifstream inFile(playerFile);

    // 1 1 1 Jerome Robinson 34 19 4 3 1.7 0.4
    string line;
    while (getline(inFile, line)) {
        stringstream ss(line);
        int playerId;
        int teamId;
        int uniformNum;
        string firstName;
        string lastName;
        int mpg;
        int ppg;
        int rpg;
        int apg;
        double spg;
        double bpg;
        if (!(ss >> playerId >> teamId >> uniformNum >> firstName >> lastName >> mpg >> ppg >> rpg >> apg >> spg >> bpg))
            break;
        
        add_player(C, teamId, uniformNum, firstName, lastName, mpg, ppg, rpg, apg, spg, bpg);
    }

    inFile.close();
}

void fillTables(connection *C) {
    fillState(C);
    cout << "Filled table STATE successfully" << endl;
    fillColor(C);
    cout << "Filled table COLOR successfully" << endl;
    fillTeam(C);
    cout << "Filled table TEAM successfully" << endl;
    fillPlayer(C);
    cout << "Filled table PLAYER successfully" << endl;
}

void add_player(connection *C, int team_id, int jersey_num, string first_name, string last_name,
                int mpg, int ppg, int rpg, int apg, double spg, double bpg) {
    static int id = 0;
    ++id;

    // sanitize the string
    first_name = sanitizeString(first_name);
    last_name = sanitizeString(last_name);

    stringstream ss;
    ss << "INSERT INTO PLAYER VALUES (" << id << ", " << team_id << ", " << jersey_num <<
    ", '" << first_name << "'" << ", '" << last_name << "'" << ", " << mpg << ", " << ppg <<
    ", " << rpg << ", " << apg << ", " << spg << ", " << bpg << ");";

    /* Create a transactional object. */
    work W(*C);

    /* Execute SQL query */
    try {
        W.exec(ss.str());
        W.commit();
        // cout << "Value inserted successfully" << endl;
    }
    catch (const exception &e) {
        cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}


void add_team(connection *C, string name, int state_id, int color_id, int wins, int losses) {
    static int id = 0;
    ++id;

    // sanitize the string
    name = sanitizeString(name);

    stringstream ss;
    ss << "INSERT INTO TEAM VALUES (" << id << ", '" << name << "'" << 
    ", " << state_id << ", " << color_id << ", " << wins << ", " << losses << ");";

    /* Create a transactional object. */
    work W(*C);

    /* Execute SQL query */
    try {
        W.exec(ss.str());
        W.commit();
        // cout << "Value inserted successfully" << endl;
    }
    catch (const exception &e) {
        cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}


void add_state(connection *C, string name) {
    static int id = 0;
    ++id;

    // sanitize the string
    name = sanitizeString(name);

    stringstream ss;
    ss << "INSERT INTO STATE VALUES (" << id << ", '" << name << "');";

    /* Create a transactional object. */
    work W(*C);

    /* Execute SQL query */
    try {
        W.exec(ss.str());
        W.commit();
        // cout << "Value inserted successfully" << endl;
    }
    catch (const exception &e) {
        cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}


void add_color(connection *C, string name) {
    static int id = 0;
    ++id;

    // sanitize the string
    name = sanitizeString(name);

    stringstream ss;
    ss << "INSERT INTO COLOR VALUES (" << id << ", '" << name << "');";

    /* Create a transactional object. */
    work W(*C);

    /* Execute SQL query */
    try {
        W.exec(ss.str());
        W.commit();
        // cout << "Value inserted successfully" << endl;
    }
    catch (const exception &e) {
        cerr << e.what() << std::endl;
        exit(EXIT_FAILURE);
    }
}


void query1(connection *C,
	    int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            )
{
}


void query2(connection *C, string team_color)
{
}


void query3(connection *C, string team_name)
{
}


void query4(connection *C, string team_state, string team_color)
{
}


void query5(connection *C, int num_wins)
{
}
