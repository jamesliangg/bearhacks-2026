import { defineConfig } from "wxt";

export default defineConfig({
  manifest: {
    name: "VIA Delay Oracle",
    description: "Predicted VIA Rail delay overlays on the booking page.",
    version: "0.1.0",
    permissions: ["storage"],
    host_permissions: [
      "https://reservia.viarail.ca/*",
      "https://www.viarail.ca/*",
      "http://localhost:8003/*"
    ],
    action: { default_title: "VIA Delay Oracle" }
  }
});
