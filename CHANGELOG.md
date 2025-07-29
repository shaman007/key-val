# Changelog

## 2025-02-01 – `af40455` by Andrey Bondarenko
- **License Update**
  - Added `LICENSE` file with GNU General Public License v3.0.

- **Documentation**
  - Updated `README.md` to reflect licensing changes (exact changes not shown in diff). 

**Note**: Ensure compliance with GPL v3.0 for future contributions and distributions.

## 2025-02-01 – `da3c241` by Andrey Bondarenko
- **Documentation**
  - Updated `README.md` to include a description of the new server functionality.

- **Server Implementation**
  - Added `server.c` implementing a simple in-memory key-value store server.
  - Server listens on port 8080, supports commands: `write key value` and `search key`.
  - Handles up to 5 concurrent clients with basic error responses for command usage.

- **Functionality**
  - Introduced functions for setting (`set_value`) and getting (`get_value`) key-value pairs.
  - Implemented command parsing and response handling for client interactions.

- **Error Handling**
  - Added error messages for invalid commands and full storage scenarios.

## 2025-02-01 – `619d334` by Andrey Bondarenko
- **CI/CD Integration**
  - Added GitHub Actions workflow for C/C++ CI in `.github/workflows/c-cpp.yml`.
  - Configures CI to run on `push` and `pull_request` events for the `main` branch.
  - Includes steps for checkout, configuration, build, testing, and distribution checks.

## 2025-02-01 – `0428885` by Andrey Bondarenko
- **Build System**  
  - Added `configure` script to generate a Makefile for building the server application.
  - Makefile includes targets for building (`server`) and cleaning (`clean`) the executable.

- **File Changes**  
  - New file: `configure` created with executable permissions.

## 2025-02-01 – `3ae49cc` by Andrey Bondarenko
- **File Management**
  - Added `.gitignore` to exclude `Makefile` from version control.

## 2025-02-01 – `d3bfb77` by Andrey Bondarenko
- **Workflow Update**
  - Changed `run: ./configure` to `run: bash ./configure` in `.github/workflows/c-cpp.yml` for explicit shell execution.

## 2025-02-01 – `773967e` by Andrey Bondarenko
- **CI Workflow Updates**
  - Removed `make check` and `make distcheck` steps from `.github/workflows/c-cpp.yml`.

## 2025-02-01 – `bab0af6` by Andrey Bondarenko
- **New File Added:**
  - Introduced `server` binary file.

## 2025-02-01 – `331ba2c` by Andrey Bondarenko
- **Feature Addition:**
  - Implemented `dump_store()` function in `server.c` to return a string representation of the key-value store.
  - Added support for the `dump` command in the main loop to send the store's contents over the socket.

- **Command Handling:**
  - Modified command parsing to allow for a single token (`num_tokens >= 1`) for the `write` command, enhancing flexibility.

- **Memory Management:**
  - Allocated memory for the dump string; ensure to handle potential `malloc` failure.

- **File Affected:**
  - `server.c` - significant changes to command processing and store dumping functionality.

## 2025-02-01 – `cc47185` by Andrey Bondarenko
- **Documentation Updates**
  - Updated `README.md` to reflect new commands: `dump` and `close` (renamed to `quit`).

- **Server Functionality Enhancements**
  - Added `dump` command to retrieve in-memory data.
  - Implemented `quit` command to close the connection gracefully. 

- **File Changes**
  - `README.md`: Expanded command list.
  - `server.c`: Logic for new commands integrated.

## 2025-02-01 – `0e80ae8` by Andrey Bondarenko
- **.gitignore Updates**
  - Added `server` to `.gitignore` to exclude server-related files from version control.

## 2025-02-01 – `e8f2f7b` by Andrey Bondarenko
- **Removed**: Entire `server` file (binary) deleted. 
- **Impact**: This change may break existing server functionality; ensure any dependent services are updated accordingly.

## 2025-02-01 – `20eaac4` by Andrey Bondarenko
- **Feature Addition**: Added support for new commands in `server.c`.
  - Commands: `dump`, `quit` documented in the main function.

## 2025-02-01 – `37a3f82` by Andrey Bondarenko
- **Data Structure Changes**
  - Replaced static array-based key-value store with a dynamic hash table implementation.
  - Introduced `Node` structure for linked list in hash table buckets.
  - Added `HashTable` structure to manage buckets and count.

- **Functionality Enhancements**
  - Implemented `create_table`, `insert`, `search`, and `resize_table` functions for hash table operations.
  - Added `free_table` and `free_list` functions for memory management.
  - Updated `dump_store` to output contents of the hash table.

- **Memory Management**
  - Utilized dynamic memory allocation for keys and values, ensuring proper cleanup with `free_table`.
  - Added error handling for memory allocation failures.

- **Breaking Changes**
  - Removed the previous `set_value` and `get_value` functions; existing code relying on these will need to be updated to use the new hash table API.

## 2025-02-01 – `e1af61a` by Andrey Bondarenko
- **Concurrency Enhancements**:
  - Added `pthread_mutex_t store_mutex` for thread safety in `server.c`.
  - Wrapped critical sections in `insert`, `search`, `resize_table`, `free_table`, and `dump_store` functions with mutex locks.

- **Client Handling**:
  - Introduced `client_info` struct to manage client socket and hash table reference.
  - Implemented `client_handler` function for processing client requests in separate threads.

- **File Changes**:
  - Created new binary file `a.out`.

- **Breaking Changes**:
  - The main server loop has been modified to support multi-threading, which may affect existing client connection handling logic.

## 2025-02-01 – `4bc7df9` by Andrey Bondarenko
- **File Deletion:**
  - Removed binary file `a.out`.

- **Server Enhancements (server.c):**
  - Increased `MAX_CLIENTS` from 5 to 521.
  - Added `created_at` timestamp to `Node` structure for tracking creation time.
  - Updated `insert` function to set `created_at` for new nodes.
  - Modified `search` function to return `Node*` instead of `char*`.
  - Adjusted `dump_store` and client response to include creation timestamp in output.

- **Potential Breaking Changes:**
  - Changes in `search` function return type may affect existing calls expecting a string.
  - Increased `MAX_CLIENTS` may require adjustments in client handling logic.

## 2025-02-01 – `91af52b` by Andrey Bondarenko
- **Concurrency Management**:
  - Removed `pthread_mutex_lock` and `pthread_mutex_unlock` calls in `resize_table` function in `server.c`.
  
- **Functionality**:
  - Adjusted hash table resizing logic by eliminating mutex locks, potentially impacting thread safety.

## 2025-02-01 – `7b4fc8e` by Andrey Bondarenko
- **Memory Management**
  - Removed `#define MAX_STORE 100` to allow dynamic sizing.
  - Updated `malloc` in `dump_store` to allocate based on `table->count` instead of a fixed size.

- **Debugging**
  - Added debug print statement for index calculation in `insert` function: `printf("Index: %ld\n", index);`.

- **Buffer Handling**
  - Adjusted `line` buffer size in `dump_store` to accommodate dynamic key and value sizes.
  - Changed `strncat` usage to append full `line` length instead of a fixed maximum. 

- **File Affected**
  - `server.c` - significant changes affecting memory allocation and debugging output.

## 2025-02-01 – `85669a7` by Andrey Bondarenko
- **Documentation Updates**
  - Updated README.md to reflect the addition of 2 new commands, increasing total from 3 to 5.

- **Build System Enhancements**
  - Modified `configure` script to check for pthreads support; exits with error if not found.
  - Updated Makefile generation to include pthread linking (`-lpthread`) and improved compiler flags (`-Wall -Wextra`).

- **Code Cleanup**
  - Removed unnecessary line in `server.c` related to freeing memory in the client handler.

## 2025-02-01 – `1e5ecbb` by Andrey Bondarenko
- **CI/CD Enhancements**
  - Added step to install build tools (`build-essential`) in `.github/workflows/c-cpp.yml`.

## 2025-02-01 – `752f338` by Andrey Bondarenko
- **CI/CD Workflow Update**
  - Updated `.github/workflows/c-cpp.yml` to include `libpthread-stubs0-dev` in the installation step.
  - Added `sudo apt-get update` before installing packages to ensure the latest versions are fetched.

## 2025-02-01 – `dcdef81` by Andrey Bondarenko
- **Workflow Update**: 
  - Added `libc-dev` to the installation step in `.github/workflows/c-cpp.yml`.
  
- **File Modified**: 
  - `.github/workflows/c-cpp.yml` updated to enhance build environment.

## 2025-02-01 – `b22bd47` by Andrey Bondarenko
- **Bug Fixes:**
  - Corrected memory allocation for `dump_store` in `server.c` to allocate 1 byte instead of a size based on `table->count`.
  
- **Code Improvements:**
  - Added braces for clarity in the `search` function's conditional statement.
  - Updated line size calculation in `dump_store` to use `strlen` for accurate buffer sizing.

- **File Affected:**
  - `server.c`

## 2025-02-01 – `30b5d90` by Andrey Bondarenko
- **Documentation Update**
  - Added a newline for formatting consistency in `README.md`.
  - Clarified command list in `README.md` by adding a period to the last item.

## 2025-02-01 – `3416a6b` by Andrey Bondarenko
- **Memory Management**
  - Added dynamic memory allocation for `dump` in `dump_store` function to accommodate variable-length strings.
  
- **Functionality**
  - Added `return NULL;` to `client_handler` function to ensure proper thread termination.

- **Code Cleanup**
  - Removed unused `buffer` variable from `main` function in `server.c`.

## 2025-02-02 – `a80bd30` by Andrey Bondarenko
- **Build System Update**
  - Added `-v` flag to the server build command in `configure` for verbose output during compilation.

## 2025-02-02 – `5b244b2` by Andrey Bondarenko
- **Configure Script Updates**
  - Added verbose flag `-v` to `gcc` command for pthreads check.
  - Removed verbose flag `-v` from server build command in `Makefile` generation.

## 2025-02-02 – `7b8d2e9` by Andrey Bondarenko
- **CI/CD Workflow Updates**
  - Added `libc6-dev` to the installation step in `.github/workflows/c-cpp.yml`.
  - Introduced a new step to check for `pthread.h` existence in the workflow.

## 2025-02-02 – `3016696` by Andrey Bondarenko
- **Configuration Update**
  - Modified `configure` script to change the order of options for `gcc` command.
  - Ensured proper error handling for pthreads library detection.

## 2025-02-02 – `ff3583b` by Andrey Bondarenko
- **Build Configuration**:
  - Updated `configure` script to remove error output redirection from `gcc` command for pthreads check.
  - Ensured clearer error reporting by allowing `gcc` verbose output during pthreads validation.

## 2025-02-02 – `b054a74` by Andrey Bondarenko
- **Configuration Script Update**
  - Changed `echo` command to use `-e` flag for proper interpretation of escape sequences in `configure` script.
  - Ensured compatibility with systems requiring explicit escape sequence handling for pthreads check.

## 2025-02-02 – `8d19bf8` by Andrey Bondarenko
- **Configuration Script Update**
  - Modified `configure` script to suppress verbose output from `gcc` when checking for pthreads support.
  - Ensured error handling remains intact by redirecting `stderr` to `/dev/null`.

## 2025-02-02 – `e76745a` by Andrey Bondarenko
- **Server Enhancements**
  - Introduced epoll for improved I/O handling with `epoll_fd` and `server_socket`.
  - Added support for multiple worker threads with `WORKER_THREADS` defined.

- **Hash Table Improvements**
  - Refactored hash table functions to use a global instance (`global_table`).
  - Updated `create_table`, `insert`, `search`, and `resize_table` functions to operate on the global hash table.
  - Enhanced memory management in `create_node` to ensure proper allocation and deallocation.

- **Utility Functions**
  - Added `set_nonblocking` to configure sockets as non-blocking.
  - Introduced `trim_newline` for cleaning input strings.

- **Breaking Changes**
  - Functions `insert`, `search`, and `resize_table` no longer accept a `HashTable` parameter; they now operate on the global hash table directly.
  - The return type of `create_table` changed from `HashTable*` to `int`, indicating success or failure.

- **File Affected**
  - `server.c` - Major structural and functional updates.

## 2025-02-02 – `156d617` by Andrey Bondarenko
- **Code Cleanup**
  - Reorganized `#include` statements for clarity and consistency in `server.c`.
  
- **Constants and Definitions**
  - Added comments to clarify the purpose of constants like `PORT`, `MAX_EVENTS`, `WORKER_THREADS`, etc.

- **Error Message Improvements**
  - Updated error messages in `read_client_data` for better readability and corrected formatting.

- **Hash Table Initialization**
  - Added comment to `create_table` function to describe its purpose.

- **File Affected**
  - `server.c` - significant changes to structure and comments, no breaking changes noted.

## 2025-02-02 – `138a5f2` by Andrey Bondarenko
- **Buffer Handling**: 
  - Added null-termination for `buffer` in `read_client_data()` to prevent potential overflow issues.
  
- **Client Response**: 
  - Introduced a response message "OK\n" to be sent to the client when dumping fails in `read_client_data()`.

- **Code Clarity**: 
  - Enhanced comments in `main()` for clarity on socket options and event structures.

- **Thread Management**: 
  - Clarified declaration of `threads` array for worker threads in `main()`. 

**Note**: No breaking changes detected.

## 2025-02-02 – `6b92f2c` by Andrey Bondarenko
- **CI/CD Enhancements**
  - Updated GitHub Actions workflow (`.github/workflows/c-cpp.yml`) to include `uuid-dev` and `libossp-uuid-dev` in build tools installation.

- **Build System Updates**
  - Modified `configure` script to check for `uuid` library support.
  - Updated `Makefile` to link against `-luuid` for both `server` and new `client` targets.

- **New Client Implementation**
  - Added `test/client.c` for client-side functionality, allowing interaction with the server.

- **Code Improvements**
  - Changed initial capacity of hash table in `server.c` from 101 to 1023.
  - Enhanced output formatting in `dump_store()` function to include an incrementing index.
  - Added logging for event processing in `worker_thread()`.

- **Bug Fixes**
  - Fixed potential buffer overflow in `read_client_data()` by ensuring proper null termination.
  - Adjusted response handling in `read_client_data()` to ensure "OK" is sent after a successful dump.

- **File Management**
  - Updated `.gitignore` to exclude `client` binary.

## 2025-02-02 – `52eedb9` by Andrey Bondarenko
- **Build System**
  - Updated `clean` target in `configure` to remove `test/client` executable.

- **Server Functionality**
  - Added logging in `read_client_data` to indicate client socket reading.
  - Enhanced logging in worker thread for event waiting in `server.c`.

- **Client Enhancements**
  - Included `<unistd.h>` in `test/client.c`.
  - Modified `send_command` to log command sent and server response received.
  - Adjusted command format in `run_sequence` to remove trailing newline.

- **Code Cleanup**
  - Removed unnecessary newline at the end of `main` function in `test/client.c`.

## 2025-02-02 – `b67be24` by Andrey Bondarenko
- **GitHub Actions Workflow**:
  - Updated `.github/workflows/c-cpp.yml`: changed `make` to `make all` in the build step.

- **Build Configuration**:
  - Modified `configure`: added `all` target to build both `server` and `client`.
  - Adjusted clean target to remove `./test/client` instead of `test/client`. 

- **Breaking Change**:
  - The default make command now requires `make all` to build all targets.

## 2025-02-02 – `36efb7b` by Andrey Bondarenko
- **CI Workflow Updates**
  - Modified GitHub Actions workflow in `.github/workflows/c-cpp.yml` to run `make` instead of `make all`.

- **Build Configuration Changes**
  - Updated `configure` script to remove escape characters from `make` commands for `server` and `client` targets.

## 2025-02-02 – `7dcce90` by Andrey Bondarenko
- **Feature Addition:**
  - Implemented "size" command in `read_client_data` to return the count of items in `global_table`.
  - Allocated memory for response using `malloc` and formatted output with `sprintf`.

- **Code Modification:**
  - Reordered command checks to prioritize "size" before "wipe" in `server.c`.

- **Memory Management:**
  - Ensure to free allocated memory for `response` after use to prevent memory leaks (not shown in the diff).

## 2025-02-02 – `34d5a97` by Andrey Bondarenko
- **Configuration Updates**
  - Modified `configure` script for improved setup process.

## 2025-02-02 – `f7fa54a` by Andrey Bondarenko
- **Documentation Updates**
  - Added "About" section to README.md detailing server functionality and commands.
  - Expanded command list to include "wipe" and "size" commands.
  - Noted limitation of "dump" command for large datasets (500,000+ key-values).
  - Introduced "Data model" section explaining the hash table structure.

## 2025-02-03 – `f649e1e` by Andrey Bondarenko
- **Server Enhancements**
  - Updated `read_client_data` in `server.c` to include `global_table->capacity` in the response for the "size" command.
  
- **Testing Improvements**
  - Modified log output in `send_command` function of `test/client.c` for clearer command indication.

## 2025-02-03 – `590f26e` by Andrey Bondarenko
- **Feature Addition:**
  - Implemented `delete` function to remove a key from the hash table, returning status codes based on success or failure.
  
- **Command Handling:**
  - Added support for `delete` command in `read_client_data`, allowing clients to delete keys from the hash table.
  - Updated error messages to include `delete` command in both valid and invalid command responses.

- **File Modified:**
  - Changes made in `server.c`.

## 2025-02-03 – `7b93bbe` by Andrey Bondarenko
- **Documentation Updates**  
  - Clarified command syntax in `README.md` for better usability:
    - Changed "write key-value" to "write key value" (adds timestamp).
    - Changed "search value by the key" to "search key".
    - Corrected "dump what is in memory" to "dump".
    - Added "delete key" command for key removal.
    - Added "quit" command to close the connection.
    - Updated memory dump limitation from 500,000 to 50,000 key-values.  
- **File Affected**  
  - `README.md` - Enhanced command descriptions and fixed typographical errors.

## 2025-02-03 – `09e750c` by Andrey Bondarenko
- **Feature Addition**
  - Added `MAX_TTL` constant to define maximum time-to-live for key-value pairs in `server.c`.
  - Introduced `ttl` field in `Node` struct to store time-to-live for each key-value pair.

- **File Modified**
  - `server.c` updated to enhance hash table functionality with TTL management.

## 2025-02-03 – `62d2e95` by Andrey Bondarenko
- **Memory Management**:
  - Replaced `malloc` with `strdup` for `key` and `value` allocation in `create_node` and `insert` functions in `server.c`.
  - Removed unnecessary `malloc` calls, simplifying memory allocation logic.

- **Hash Table Resizing**:
  - Updated growth strategy in `resize_table` function: changed new capacity calculation from `old_capacity * 2 + 1` to `old_capacity * 2`. 

- **File Affected**:
  - `server.c` - Significant changes to memory allocation and hash table resizing logic.

## 2025-02-03 – `8f04a7f` by Andrey Bondarenko
- **Documentation Update**
  - Corrected spelling of "folowing" to "following" in README.md.

## 2025-02-04 – `7f71918` by Andrey Bondarenko
- **Data Structure Update**
  - Added `unsigned long hash` field to `Node` structure in `server.c`.

- **Function Signature Changes**
  - Updated `create_node` to accept `unsigned long h` parameter.
  - Modified `insert` and `search` functions to utilize the new hash field.

- **Hash Calculation Optimization**
  - Changed hash index calculation to store hash value in `Node` for reuse during insertion and searching.

- **Debugging Enhancements**
  - Added debug print statements to log insertion index and node hash during operations.

- **Memory Management**
  - Adjusted memory allocation for `dump_store` to accommodate additional hash information in output.

- **Breaking Change**
  - The `create_node` function now requires an additional parameter, affecting all calls to this function.

## 2025-02-04 – `28143b2` by Andrey Bondarenko
- **Server Code Changes (server.c)**:
  - Removed debug `printf` statements for index logging in `insert` and `search` functions.
  - Updated `delete` function signature from `int *delete` to `int delete`.
  - Enhanced key deletion logic to use hash comparison in `delete` function.
  - Commented out debug `printf` statements in `read_client_data` and `worker_thread`.

- **Client Test Code Changes (test/client.c)**:
  - Added `#define DEBUG` for conditional debug logging.
  - Introduced debug `printf` statements in `send_command` to log sent commands and received responses.
  - Removed debug `printf` for sending buffer in `run_sequence`.

- **Breaking Changes**:
  - The `delete` function now returns an `int` instead of an `int *`, which may affect existing calls to this function.

## 2025-02-04 – `ec922f9` by Andrey Bondarenko
- **Documentation Updates**
  - Updated `README.md` to clarify that the "dump" command fails on 1000+ key-values (previously 50,000+).
  - Added a "Known problems" section detailing:
    - Potential server segfault if the client spams commands without reading the socket.
    - Limitation of the "dump()" function to a few thousand records.

## 2025-02-04 – `1c8d42f` by Andrey Bondarenko
- **Documentation Updates**
  - Revised content in `README.md` for clarity and accuracy.

## 2025-02-04 – `d015735` by Andrey Bondarenko
- **Documentation Updates**
  - Updated `README.md` to clarify the `dump` command functionality, now supporting `dump index offset` syntax.
  - Revised data model description to include hash information.

- **Functionality Enhancements**
  - Modified `dump_store` function in `server.c` to accept `index` and `offset` parameters, allowing for more flexible data retrieval.
  - Improved error handling for invalid `index` or `offset` in `dump_store`.

- **Testing Improvements**
  - Increased random string length in `test/client.c` from 64 to 512 characters for more robust testing scenarios. 

- **Breaking Changes**
  - The `dump_store` function signature has changed; it now requires two parameters (`size_t index, size_t offset`). Update any calls to this function accordingly.

## 2025-02-04 – `9f03934` by Andrey Bondarenko
- **Feature Enhancements:**
  - Added "add key value" command to write key without overwriting existing values.
  - Introduced "update key value" command to overwrite existing keys.

- **Code Changes:**
  - Updated `insert` function to accept a `type` parameter for operation differentiation.
  - Changed `ttl` type from `int` to `unsigned long` in `Node` structure.
  - Replaced `calloc` with `malloc` for bucket allocation in `create_table` and `resize_table`.

- **Error Handling:**
  - Enhanced error responses for "add" and "update" commands in `read_client_data`.

- **File Modifications:**
  - Updated `README.md` to reflect new command functionalities.
  - Significant changes in `server.c` affecting data insertion logic and memory allocation.

## 2025-02-04 – `cc06db9` by Andrey Bondarenko
- **Documentation Updates**
  - Updated README.md to clarify "dump index offset" command behavior, specifying it returns first `INITIAL_CAPACITY` indexes instead of 100.

- **Code Changes**
  - Modified `server.c` to set default `offset` to `INITIAL_CAPACITY - 1` instead of a hardcoded value of 1022 in `read_client_data` function. 

- **Breaking Change**
  - The behavior of the "dump" command has changed; it now returns a variable number of indexes based on `INITIAL_CAPACITY`.

## 2025-02-06 – `05152a0` by Andrey Bondarenko
- **Documentation Updates**
  - Added "ToDo" section in `README.md` outlining future enhancements:
    - Key expiration on access
    - Persistence strategy considerations (Periodic Snapshots + WAL)
    - Simple master-master replication with Last-Write-Wins (LWW) strategy.

## 2025-02-06 – `33cafb7` by Andrey Bondarenko
- **Documentation Update**
  - Added "Configfile, cli keys" to ToDo section in `README.md`.

## 2025-02-06 – `2e163a1` by Andrey Bondarenko
- **Documentation Updates:**
  - Added "Dokerise" to project features in `README.md`.
  - Included "Test automations" and "Consistent logging" in project features.
  
- **Known Issues:**
  - Clarified known issue regarding client behavior leading to server segfaults in `README.md`.

## 2025-02-07 – `118045a` by Andrey Bondarenko
- **Documentation Updates**
  - Updated `README.md` to reflect changes in command syntax, adding optional `ttl` parameter for `write`, `add`, and `update` commands.

- **Server Logic Enhancements**
  - Changed `ttl` type in `Node` struct from `unsigned long` to `time_t` in `server.c`.
  - Modified `insert` function to accept `size_t ttl` parameter.
  - Implemented TTL checks in `search` function to handle expired keys.
  - Added `garbage_collect` function to remove expired keys from the hash table.

- **Client Interaction**
  - Updated `read_client_data` function to parse optional `ttl` from client commands.

- **Build Configuration**
  - Added `-g` flag to `CFLAGS` in `configure` for debugging support.

- **Breaking Changes**
  - The `insert` function signature has changed; it now requires a `ttl` parameter, which may affect existing client code.

## 2025-05-16 – `23c49ba` by Andrey Bondarenko
- **Memory Management Enhancements**
  - Added error handling for `malloc` in `server.c` to prevent crashes on allocation failure.

- **New Feature Implementation**
  - Introduced `test.c`, a new file implementing a scalable TCP server using epoll and pthreads, listening on port 8080.
  - Added worker threads for handling connections, improving concurrency.

- **Code Cleanup**
  - Freed allocated memory for `dump` in `server.c` after use to prevent memory leaks.

## 2025-05-16 – `64ee633` by Andrey Bondarenko
- **Memory Management Enhancements**
  - Replaced `malloc` with `calloc` for node allocation in `create_node` to ensure zero-initialization.
  - Improved error handling in `create_node` by freeing the correct pointers on allocation failure.
  - Added `created_at` timestamp to nodes during creation.

- **Table Resizing Improvements**
  - Changed growth strategy from doubling to tripling the capacity in `resize_table`.
  - Utilized `calloc` for new buckets allocation to ensure zero-initialization.

- **Thread Safety and Error Handling**
  - Introduced mutex locks around critical sections in `resize_table` and `free_table` to ensure thread safety.
  - Added checks for null pointers in `free_table` to prevent dereferencing null pointers.

- **Dynamic Buffer Management**
  - Implemented dynamic resizing of the dump buffer in `dump_store` to handle varying sizes of output more efficiently.

- **Code Cleanliness**
  - Removed unnecessary free calls and improved readability by adding comments and restructuring code blocks.

## 2025-05-16 – `695166c` by Andrey Bondarenko
- **Memory Management**
  - Fixed memory allocation in `resize_table()` in `server.c`: corrected `calloc` parameters from `new_capacity * sizeof(Node *)` to `new_capacity` for proper allocation size.
  
- **Concurrency**
  - Ensured thread safety with `pthread_mutex_lock` during table resizing.

## 2025-05-16 – `033f3bd` by Andrey Bondarenko
- **Error Handling Enhancements**  
  - Added handling for `EINTR` in `epoll_wait`, allowing the loop to continue on signal interruptions.  
  - Introduced critical error exit on `epoll_wait` failure.

- **Event Processing Improvements**  
  - Implemented checks for `EPOLLERR`, `EPOLLHUP`, and `EPOLLRDHUP` to manage client socket errors and hang-ups, ensuring proper closure of connections.  
  - Clarified event handling by skipping the server socket in the event loop.

- **Code Cleanup**  
  - Removed commented-out debug print statements for cleaner code.  

**File Affected:** `server.c`  
**Note:** No breaking changes identified.

## 2025-05-16 – `2eea8c4` by Andrey Bondarenko
- **Functionality Enhancements in `read_client_data`**:
  - Improved error handling for client disconnections and read errors.
  - Simplified TTL value determination using a conditional operator.
  - Optimized memory usage by replacing dynamic allocation with stack allocation for response buffers.

- **Command Handling Improvements**:
  - Enhanced command parsing logic for `dump` and `size` commands.
  - Added error handling for dump failures and ensured consistent response formatting.

- **Code Cleanup**:
  - Removed unnecessary comments and redundant code segments.
  - Streamlined buffer management and response writing.

- **File Affected**:
  - `server.c` - significant changes to client data handling and command processing logic.
