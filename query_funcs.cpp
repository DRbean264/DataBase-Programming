#include "query_funcs.h"
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <iomanip>

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

template<typename T>
string query1Helper(string attribute, T minValue, T maxValue, bool first) {
    stringstream ss;
    
    if (!first)
        ss << " and ";
    else
        ss << " where ";
    
    ss << attribute << " between " << minValue << " and " << maxValue;
    return ss.str();
}

// show all attributes of each player with average statistics that fall between the
// min and max (inclusive) for each enabled statistic
void query1(connection *C,
	        int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            ) {
    // get query
    stringstream ss;
    ss << "select * from player";

    bool first = true;
    if (use_mpg) {
        ss << query1Helper<int>("mpg", min_mpg, max_mpg, first);
        first = false;
    }
    if (use_ppg) {
        ss << query1Helper<int>("ppg", min_ppg, max_ppg, first);
        first = false;
    }
    if (use_rpg) {
        ss << query1Helper<int>("rpg", min_rpg, max_rpg, first);
        first = false;
    }
    if (use_apg) {
        ss << query1Helper<int>("apg", min_apg, max_apg, first);
        first = false;
    }
    if (use_spg) {
        ss << query1Helper<double>("spg", min_spg, max_spg, first);
        first = false;
    }
    if (use_bpg) {
        ss << query1Helper<double>("bpg", min_bpg, max_bpg, first);
        first = false;
    }
    
    ss << ";";

    nontransaction N(*C);
    
    try {
        /* Execute SQL query */
        result R(N.exec(ss.str()));
        
        /* List down all the records */
        cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG\n";
        for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
            std::ios_base::fmtflags f(cout.flags());

            cout << c[0].as<int>() << ' ' << c[1].as<int>() << ' ' << c[2].as<int>() << ' ' <<
            c[3].as<string>() << ' ' << c[4].as<string>() << ' ' << c[5].as<int>() << ' ' <<
            c[6].as<int>() << ' ' << c[7].as<int>() << ' ' << c[8].as<int>() << ' ' <<
            fixed << setprecision(2) << c[9].as<double>() << ' ' << c[10].as<double>() << endl;

            cout.flags(f);
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}

//  show the name of each team with the indicated uniform color
void query2(connection *C, string team_color) {
    // get query
    stringstream ss;
    ss << "select t.name from team as t, color as c where t.color_id = c.color_id and c.name = ";
    ss << "'" << team_color << "'" << ';';

    nontransaction N(*C);
    
    try {
        /* Execute SQL query */
        result R(N.exec(ss.str()));
        
        /* List down all the records */
        cout << "NAME\n";
        for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
            cout << c[0].as<string>() << endl;
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}

// show the first and last name of each player that plays for the indicated team,
// ordered from highest to lowest ppg (points per game)
void query3(connection *C, string team_name) {
    // get query
    stringstream ss;
    ss << "select p.first_name, p.last_name from player as p, team as t where p.team_id = t.team_id" <<
    " and t.name = '" << team_name << "' order by p.ppg desc" << ';';

    nontransaction N(*C);
    
    try {
        /* Execute SQL query */
        result R(N.exec(ss.str()));
        
        /* List down all the records */
        cout << "FIRST_NAME LAST_NAME\n";
        for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
            cout << c[0].as<string>() << ' ' << c[1].as<string>() << endl;
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}

// show first name, last name, and jersey number of each player that plays in the
// indicated state and wears the indicated uniform color
void query4(connection *C, string team_state, string team_color) {
    // get query
    stringstream ss;
    ss << "select p.first_name, p.last_name, p.uniform_num from player as p, team as t, state as s, color as c" <<
    " where p.team_id = t.team_id and t.state_id = s.state_id and t.color_id = c.color_id" <<
    " and s.name = '" << team_state << "'" << " and c.name = '" << team_color << "';";

    nontransaction N(*C);
    
    try {
        /* Execute SQL query */
        result R(N.exec(ss.str()));
        
        /* List down all the records */
        cout << "FIRST_NAME LAST_NAME UNIFORM_NUM\n";
        for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
            cout << c[0].as<string>() << ' ' << c[1].as<string>() << ' ' << c[2].as<int>() << endl;
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}

// show first name and last name of each player, and team name and number of
// wins for each team that has won more than the indicated number of games
void query5(connection *C, int num_wins) {
    // get query
    stringstream ss;
    ss << "select p.first_name, p.last_name, t.name, t.wins from player as p, team as t" <<
    " where p.team_id = t.team_id" << " and t.wins > " << num_wins << ';';

    nontransaction N(*C);
    
    try {
        /* Execute SQL query */
        result R(N.exec(ss.str()));
        
        /* List down all the records */
        cout << "FIRST_NAME LAST_NAME NAME WINS\n";
        for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
            cout << c[0].as<string>() << ' ' << c[1].as<string>() << ' ' << c[2].as<string>() << ' ' <<
            c[3].as<int>() << endl;
        }
    }
    catch(const std::exception& e) {
        std::cerr << e.what() << '\n';
        exit(EXIT_FAILURE);
    }
}
