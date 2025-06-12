# Tork Devcontainer

This devcontainer provides a quick and easy setup for anyone using VSCode to get up and running quickly with Tork development. It bootstraps a docker container for you to develop inside of without the need to manually setup the environment.

---

## INSTRUCTIONS

### Setup:

Once you have this repo cloned to your local system, you will need to install the VSCode extension [Remote Development](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack).

Then run the following command from the command palette:
`Dev Containers: Open Folder in Container...` and select your Tork root folder.

This will then put you inside a docker container ready for development.

---

### Development:

It is recommended to read the docs here for some general guidance and more info regarding the platform: https://www.tork.run/quick-start

Make a copy of the `sample.config.toml` file within `configs/` and paste it in the root of the project and name it `config.toml`. Then change all `host=` values  from `localhost` to `host.docker.internal`.

The next step is to run docker compose as this will get you set up with some local services to develop using e.g. postgres database etc.

Make sure to run the following command from your host system to start the docker compose stack as you cannot run docker within docker, so navigate to this folder within your host system and run the command:

```bash
$ docker compose down -v && docker compose up -d
```

From this point, you can start developing.

---

### GIT

If you want to commit to GitHub, make sure to navigate to the `~/.ssh` folder and either create a new SSH key or override the existing `id_ed25519` file and paste an existing SSH key from your local machine into this file. You will then need to change the permissions of the file by running: `chmod 600 id_ed25519`. This will allow you to then push to GitHub.

---
