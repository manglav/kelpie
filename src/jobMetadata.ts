import { Task } from "./types";

export type JobMetadata = {
  command: string;
  argument_count: number;
  run_name: string | null;
  shard: number | null;
  s3_output: string | null;
  work_dir: string | null;
};

function asStringArgs(args: any[]): string[] {
  return args.map((arg) => String(arg));
}

function optionValue(args: string[], name: string): string | null {
  const index = args.indexOf(name);
  if (index < 0 || index + 1 >= args.length) {
    return null;
  }
  return args[index + 1];
}

export function extractRunNameFromS3Output(s3Output: string | null): string | null {
  if (!s3Output) {
    return null;
  }
  const match = s3Output.match(/\/runs\/(.+?)\/outputs\/?$/);
  return match ? match[1] : null;
}

export function extractJobMetadata(work: Task): JobMetadata {
  const args = asStringArgs(work.arguments ?? []);
  const shardRaw = optionValue(args, "--shard-index");
  const shard =
    shardRaw !== null && /^-?\d+$/.test(shardRaw) ? Number.parseInt(shardRaw, 10) : null;
  const s3Output = optionValue(args, "--s3-output");

  return {
    command: work.command,
    argument_count: args.length,
    run_name: extractRunNameFromS3Output(s3Output),
    shard,
    s3_output: s3Output,
    work_dir: optionValue(args, "--work-dir"),
  };
}
