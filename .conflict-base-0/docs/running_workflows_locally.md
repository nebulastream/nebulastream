# How-To-Act
It is possible to run the `.github/workflows/act.yml` file locally. 
This is useful for testing the workflow before pushing it to the repository.
The following steps describe in a high-level how to run the workflow locally.
We will go into more detail in the following sections.

1. Install the [act](https://github.com/nektos/act) tool.
2. Create a `event.json` file that contains `{"act": true}`.
3. Run the `act` command in the terminal to trigger the workflow.


## Install the act tool
The `act` tool is a GitHub Actions runner that can be used to run workflows locally.
There are different ways to install the `act` tool. The easiest is to install it as a [GitHub CLI](https://cli.github.com/) extension.
We assume that you have already installed the GitHub CLI. If not, you can find the installation instructions [here](https://github.com/cli/cli#installation).
After installing the GitHub CLI, you can install the `act` tool by running the following command in the terminal:
```bash
gh extension install nektos/gh-act
```

## Create the event.json file
The `event.json` file is used to trigger the workflow. It contains an event that tells the workflow to skip some steps that are not relevant for local testing.
Create a file named `event.json` in the root directory of the repository that contains the following content:`{ "act": true }`. 
One way to create the `event.json` file is to run the following command:
```bash
printf "\
{\n\
\"act\": true\n\
}\n" > event.json
```

## Run the act command
After installing the `act` tool and creating the `event.json` file, you can run the workflow by executing the command below in the terminal.
We assume that you call the command from the root directory of the repository.
This command will run the workflow defined in the `.github/workflows/pr.yml` file and built it for the `x64` platform:
```bash
gh act -W .github/workflows/pr.yml -P self-hosted=catthehacker/ubuntu:runner-latest  --matrix arch:x64 -e event.json
```
For running the workflow for `arm` platform, you can use the following command:
```bash
gh act -W .github/workflows/pr.yml -P self-hosted=catthehacker/ubuntu:runner-latest  --matrix arch:arm64 -e event.json
```


## Links
- https://github.com/nektos/act
- https://nektosact.com/
- https://nektosact.com/installation/gh.html
- https://cli.github.com/
