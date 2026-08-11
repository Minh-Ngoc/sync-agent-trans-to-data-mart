module.exports = {
  apps: [{
    name: "sync-data-fv-agent_trans",
    script: "./main.js",
    node_args: "--env-file=.env"
  }]
};