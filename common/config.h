/**
 * A set of global variables that act as the config, until the real config.yml part
 * is integrated.
 *
 */

#include <chrono>
std::string DEFAULT_COMMITLOG_DIRECTORY = "logs";
long int LOG_BUFFER_SIZE = 70;
std::chrono::duration<long long int, std::milli> LOG_TIMEOUT = std::chrono::milliseconds(1000);