import { defineBuildConfig } from "obuild/config";

export default defineBuildConfig({
  entries: [
    {
      type: "bundle",
      input: ["./src/index.ts", "./src/uns3.ts"],
      rolldown: {
        platform: "neutral",
        external: ["uns3"],
      },
    },
  ],
});
