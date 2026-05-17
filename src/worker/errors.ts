import { JOB_STAGE } from '../constants';

export type WorkerErrorCode =
  | 'insufficient_worker_disk'
  | 'worker_capacity_unavailable'
  | 'resource_wait_timeout'
  | 'capacity_exceeded'
  | 'file_too_large'
  | 'source_missing'
  | 'corrupt_input'
  | 'transient_processing_failure'
  | 'duplicate_active_file_version';

export class WorkerError extends Error {
  code: WorkerErrorCode;
  readonly publicMessage: string;
  readonly retryable: boolean;

  constructor(
    code: WorkerErrorCode,
    message: string,
    options?: { publicMessage?: string; retryable?: boolean },
  ) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.publicMessage = options?.publicMessage || message;
    this.retryable = options?.retryable ?? false;
  }
}

export class ResourceUnavailableError extends WorkerError {
  readonly stage: string;
  readonly waitReason: string;

  constructor(message: string, stage: string, waitReason: string) {
    super('worker_capacity_unavailable', message, {
      publicMessage: 'Worker resources are temporarily unavailable',
      retryable: true,
    });
    this.stage = stage;
    this.waitReason = waitReason;
  }
}

export class DiskUnavailableError extends ResourceUnavailableError {
  constructor() {
    super(
      'Insufficient free worker disk to start this job right now',
      JOB_STAGE.WAITING_FOR_DISK,
      'insufficient_worker_disk',
    );
    this.code = 'insufficient_worker_disk';
  }
}

export class WorkerCapacityExceededError extends WorkerError {
  constructor(code: 'capacity_exceeded' | 'file_too_large', message: string, publicMessage?: string) {
    super(code, message, {
      publicMessage: publicMessage || 'File is too large for the current worker capacity',
      retryable: false,
    });
  }
}

export class ResourceWaitTimeoutError extends WorkerError {
  constructor() {
    super('resource_wait_timeout', 'Resource wait timeout exceeded', {
      publicMessage: 'Worker capacity is unavailable right now. Please retry later.',
      retryable: false,
    });
  }
}

export class CorruptInputError extends WorkerError {
  constructor(message: string) {
    super('corrupt_input', message, {
      publicMessage: 'Invalid or unsupported media file',
      retryable: false,
    });
  }
}

export class SourceMissingError extends WorkerError {
  constructor(message = 'Source object not found') {
    super('source_missing', message, {
      publicMessage: 'Source file was not found',
      retryable: false,
    });
  }
}

export class DuplicateActiveFileVersionError extends WorkerError {
  constructor() {
    super('duplicate_active_file_version', 'Another active job exists for this file version', {
      publicMessage: 'Another active job is already processing this file version',
      retryable: false,
    });
  }
}

export class TransientProcessingError extends WorkerError {
  constructor(message: string) {
    super('transient_processing_failure', message, {
      publicMessage: 'Processing failed',
      retryable: true,
    });
  }
}
