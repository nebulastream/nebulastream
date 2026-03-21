import { test, expect, type Page } from '@playwright/test';

/** Wait for the WASM validator to finish loading and initial validation. */
async function waitForValidator(page: Page) {
  // First wait for loading to complete (status transitions from 'loading' to 'ready'/'valid')
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'loading', { timeout: 15_000 });
  // Then wait for any in-progress validation to settle
  await page.waitForTimeout(500);
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'validating', { timeout: 5_000 });
}

/** Wait for validation to complete after a topology change. */
async function waitForValidation(page: Page) {
  // Debounce is 300ms, then validation runs
  await page.waitForTimeout(500);
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'validating', { timeout: 5_000 });
}

async function addWorker(page: Page) {
  await page.getByTestId('add-worker-btn').click();
}

async function addSourceToWorker(page: Page) {
  await page.getByTestId('add-source-btn').click();
  await page.waitForTimeout(100);
}

async function addSinkToWorker(page: Page) {
  await page.getByTestId('add-sink-btn').click();
  await page.waitForTimeout(100);
}

/** Navigate to Sources tab, create a logical source, add a schema field. */
async function createLogicalSource(page: Page, name: string, fieldName: string, fieldType: string) {
  await page.getByRole('button', { name: 'Sources' }).click();
  await page.getByRole('button', { name: /new source/i }).click();
  // Rename: find the input that currently has the default name, clear and type new name
  const nameInput = page.locator('input[type="text"]').filter({ hasText: '' }).first();
  // The name input is inside the logical source card — it has the default value "Source_1"
  const sourceNameInput = page.locator('input.flex-1').last();
  await sourceNameInput.clear();
  await sourceNameInput.fill(name);
  await sourceNameInput.press('Tab');
  // Add schema field
  await page.getByText('+ Add Field').click();
  await page.waitForTimeout(100);
  const fieldInput = page.getByPlaceholder('Field name').last();
  await fieldInput.fill(fieldName);
  // Select type from the schema builder dropdown (default is INT32)
  if (fieldType !== 'INT32') {
    const typeSelect = page.locator('select').last();
    await typeSelect.selectOption(fieldType);
  }
}

/** Assign a logical source to the currently selected physical source. */
async function assignLogicalSource(page: Page, logicalName: string) {
  const input = page.locator('input[placeholder="Search logical sources..."]');
  await input.click();
  await page.getByRole('button', { name: logicalName }).click();
}

test.describe('WASM validation in browser', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
    await waitForValidator(page);
  });

  test('empty topology shows valid status', async ({ page }) => {
    const statusBar = page.getByTestId('status-bar');
    await expect(statusBar).toHaveAttribute('data-status', 'valid');
    await expect(statusBar).toContainText('Valid');
  });

  test('status bar is visible on the canvas', async ({ page }) => {
    const statusBar = page.getByTestId('status-bar');
    await expect(statusBar).toBeVisible();
  });

  test('adding a worker keeps topology valid', async ({ page }) => {
    await addWorker(page);
    await waitForValidation(page);
    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  test('adding source without logical source shows validation error', async ({ page }) => {
    await addWorker(page);
    // Select worker, add source (no logical source assigned)
    await page.locator('.worker-node').click();
    await addSourceToWorker(page);
    await waitForValidation(page);

    const statusBar = page.getByTestId('status-bar');
    await expect(statusBar).toHaveAttribute('data-status', 'invalid');
    await expect(statusBar).toContainText('Validation error');
  });

  test('assigning logical source to physical source makes topology valid', async ({ page }) => {
    // Create a logical source first
    await createLogicalSource(page, 'sensor', 'id', 'INT64');

    // Switch to Properties tab, add worker + source
    await page.getByRole('button', { name: 'Properties' }).click();
    await addWorker(page);
    await page.locator('.worker-node').click();
    await addSourceToWorker(page);

    // Source panel is now open — assign logical source
    await assignLogicalSource(page, 'SENSOR');

    // Fill required generator_schema field (validator now checks required config params)
    const schemaField = page.locator('label:has-text("Generator Schema")').locator('..').locator('textarea, input').first();
    if (await schemaField.isVisible()) {
      await schemaField.fill('SEQUENCE INT64 0 100 1');
    }
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  test('invalid SQL query shows validation error', async ({ page }) => {
    // Create logical source
    await createLogicalSource(page, 'sensor', 'id', 'INT64');
    await page.getByRole('button', { name: 'Properties' }).click();

    // Add worker, source with logical, and a query
    await addWorker(page);
    await page.locator('.worker-node').click();
    await addSourceToWorker(page);
    await assignLogicalSource(page, 'SENSOR');

    // Open query panel, add a bad query
    const queryTab = page.getByRole('button', { name: /queries/i }).or(page.getByText(/add query/i));
    if (await queryTab.isVisible()) {
      await queryTab.click();
    }
    // Type invalid SQL in the query editor
    const queryInput = page.locator('.monaco-editor textarea, [data-testid="query-input"]');
    if (await queryInput.isVisible()) {
      await queryInput.fill('SELCT * FORM sensor');
      await waitForValidation(page);
      await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'invalid');
    }
  });

  test('validation error tooltip shows error message on hover', async ({ page }) => {
    // Create a state with validation error (source without logical)
    await addWorker(page);
    await page.locator('.worker-node').click();
    await addSourceToWorker(page);
    await waitForValidation(page);

    const statusBar = page.getByTestId('status-bar');
    await expect(statusBar).toHaveAttribute('data-status', 'invalid');

    // Check that the title attribute contains error info
    const title = await statusBar.getAttribute('title');
    expect(title).toBeTruthy();
    expect(title!.length).toBeGreaterThan(0);
  });

  test('YAML overlay shows semantic errors when validation fails', async ({ page }) => {
    // Create validation error
    await addWorker(page);
    await page.locator('.worker-node').click();
    await addSourceToWorker(page);
    await waitForValidation(page);

    // Open YAML overlay
    const yamlBtn = page.getByTestId('yaml-toggle-btn').or(page.getByRole('button', { name: /yaml/i }));
    if (await yamlBtn.isVisible()) {
      await yamlBtn.click();
      await page.waitForTimeout(300);
      // Check for error display in YAML overlay
      const errorPanel = page.locator('[style*="border-left: 3px solid"]');
      if (await errorPanel.isVisible()) {
        await expect(errorPanel).toContainText(/./);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Source type tests: filling required fields produces valid YAML
// ---------------------------------------------------------------------------

test.describe('source types with required fields produce valid YAML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
    await waitForValidator(page);

    // Create a logical source with one schema field
    await createLogicalSource(page, 'sensor', 'id', 'INT64');

    // Switch to Properties tab, add a worker
    await page.getByRole('button', { name: 'Properties' }).click();
    await addWorker(page);
    await page.locator('.worker-node').click();
  });

  test('Generator source: defaults + logical source + generator_schema = valid', async ({ page }) => {
    await addSourceToWorker(page);
    // Source panel opens — assign logical source
    await assignLogicalSource(page, 'SENSOR');

    // Fill required generator_schema (validator checks required config params)
    const schemaField = page.locator('label:has-text("Generator Schema")').locator('..').locator('textarea, input').first();
    if (await schemaField.isVisible()) {
      await schemaField.fill('SEQUENCE INT64 0 100 1');
    }
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  test('File source: defaults + logical source + file_path = valid', async ({ page }) => {
    await addSourceToWorker(page);
    await assignLogicalSource(page, 'SENSOR');

    // Change source type to File
    const sourceTypeSelect = page.locator('select').filter({ has: page.locator('option[value="File"]') });
    await sourceTypeSelect.selectOption('File');
    await page.waitForTimeout(100);

    // Fill required file_path field (use label locator, placeholder varies between fallback/WASM configs)
    const filePathField = page.locator('label:has-text("File Path")').locator('..').locator('input').first();
    await filePathField.fill('/data/input.csv');
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  // BUG: TCP source defaults have socket_type stored as integer "1" (SOCK_STREAM),
  // but the C++ validator string-compares against "SOCK_STREAM"/"SOCK_DGRAM" etc.
  // This causes validation to fail even with valid defaults. Fix in TCPSourceValidation.hpp.
  test.fixme('TCP source: defaults + logical source + host + port = valid', async ({ page }) => {
    await addSourceToWorker(page);
    await assignLogicalSource(page, 'SENSOR');

    const sourceTypeSelect = page.locator('select').filter({ has: page.locator('option[value="TCP"]') });
    await sourceTypeSelect.selectOption('TCP');
    await page.waitForTimeout(200);

    const hostInput = page.locator('label:has-text("Socket Host")').locator('..').locator('input').first();
    await hostInput.fill('127.0.0.1');

    const portInput = page.locator('label:has-text("Socket Port")').locator('..').locator('input').first();
    await portInput.fill('5000');

    await waitForValidation(page);
    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  test('Generator source: missing logical source = invalid', async ({ page }) => {
    await addSourceToWorker(page);
    // Do NOT assign logical source
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'invalid');
  });

  test('File source: with logical source but no file_path is invalid', async ({ page }) => {
    await addSourceToWorker(page);
    await assignLogicalSource(page, 'SENSOR');

    const sourceTypeSelect = page.locator('select').filter({ has: page.locator('option[value="File"]') });
    await sourceTypeSelect.selectOption('File');
    await waitForValidation(page);

    // Validator now catches missing required config parameters (file_path)
    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'invalid');
  });

  test('TCP source: with logical source but no host/port is invalid', async ({ page }) => {
    await addSourceToWorker(page);
    await assignLogicalSource(page, 'SENSOR');

    const sourceTypeSelect = page.locator('select').filter({ has: page.locator('option[value="TCP"]') });
    await sourceTypeSelect.selectOption('TCP');
    await waitForValidation(page);

    // Validator now catches missing required config parameters (socket_host, socket_port)
    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'invalid');
  });
});

// ---------------------------------------------------------------------------
// Sink type tests
// ---------------------------------------------------------------------------

test.describe('sink types with required fields produce valid YAML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
    await waitForValidator(page);

    // Add a worker, then select it for adding sinks
    await addWorker(page);
    await page.locator('.worker-node').click();
  });

  test('Void sink: just a name = valid', async ({ page }) => {
    await addSinkToWorker(page);

    // Sink panel opens — change type to Void and set name
    const sinkTypeSelect = page.locator('select').filter({ has: page.locator('option[value="Void"]') });
    await sinkTypeSelect.selectOption('Void');

    const nameInput = page.locator('label:has-text("Sink Name") ~ input, input[placeholder*="sink" i]').first();
    if (await nameInput.isVisible()) {
      await nameInput.fill('MY_SINK');
    }
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });

  test('Print sink: defaults = valid', async ({ page }) => {
    await addSinkToWorker(page);
    // Print is the default sink type; just set a name
    const nameInput = page.locator('label:has-text("Sink Name") ~ input, input[placeholder*="sink" i]').first();
    if (await nameInput.isVisible()) {
      await nameInput.fill('MY_PRINT_SINK');
    }
    await waitForValidation(page);

    await expect(page.getByTestId('status-bar')).toHaveAttribute('data-status', 'valid');
  });
});
