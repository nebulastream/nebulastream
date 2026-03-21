import { test, expect, type Page } from '@playwright/test';

async function waitForValidator(page: Page) {
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'loading', { timeout: 15_000 });
  await page.waitForTimeout(500);
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'validating', { timeout: 5_000 });
}

const PRINT_SINK_TOPOLOGY = `\
query: |
  SELECT * FROM SENSOR INTO PRINT_SINK
sinks:
  - name: PRINT_SINK
    schema:
      - name: SENSOR$ID
        type: INT64
      - name: SENSOR$VALUE
        type: FLOAT64
      - name: SENSOR$TIMESTAMP
        type: INT64
    type: Print
    host: "w1:8080"
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
      - name: value
        type: FLOAT64
      - name: timestamp
        type: INT64
physical:
  - logical: sensor
    type: Generator
    host: "w1:8080"
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
workers:
  - host: "w1:8080"
    data: "w1:9090"
    downstream: []
`;

/** Set Monaco editor content via model.setValue (fires onDidChangeContent → onChange). */
async function setMonacoContent(page: Page, content: string) {
  await page.evaluate((yaml) => {
    const editors = (window as any).monaco?.editor?.getEditors?.();
    if (editors?.[0]) {
      editors[0].getModel()?.setValue(yaml);
    }
  }, content);
}

test.describe('YAML import updates sidebar panels', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
    await waitForValidator(page);
  });

  test('pasting YAML with Print sink sets sink type dropdown to Print', async ({ page }) => {
    // Open the YAML overlay and wait for Monaco
    await page.getByTestId('yaml-toggle-btn').click();
    await page.waitForSelector('.monaco-editor', { timeout: 5_000 });
    await page.waitForTimeout(500);

    // Paste the YAML topology into the editor
    await setMonacoContent(page, PRINT_SINK_TOPOLOGY);
    await page.waitForTimeout(2000); // debounce + store update

    // Close YAML overlay
    await page.keyboard.press('Escape');
    await page.waitForTimeout(500);

    // Click the sink node (renders with "Print" label on the canvas)
    const sinkNode = page.locator('.sink-node');
    await expect(sinkNode).toBeVisible({ timeout: 5_000 });
    await sinkNode.click();
    await page.waitForTimeout(300);

    // The sidebar should show the sink properties panel
    await expect(page.locator('text=Sink Name')).toBeVisible();

    // Verify the sink name is "PRINT_SINK"
    const nameInput = page.locator('input[type="text"]').first();
    await expect(nameInput).toHaveValue('PRINT_SINK');

    // Verify the sink type dropdown shows "Print" (not "NETWORK" or another default)
    const sinkTypeSelect = page.locator('select').first();
    await expect(sinkTypeSelect).toHaveValue('Print');
  });
});
