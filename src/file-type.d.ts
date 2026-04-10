declare module 'file-type' {
  export interface FileTypeResult {
    ext: string;
    mime: string;
  }

  export function fileTypeFromFile(
    path: string
  ): Promise<FileTypeResult | undefined>;
}
