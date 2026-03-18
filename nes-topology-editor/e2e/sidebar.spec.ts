import { test, expect, type Page, type Locator } from '@playwright/test';

/** Add a worker via the toolbar */
async function addWorker(page: Page) {
  await page.getByTestId('add-worker-btn').click();
}

/** Add a source via the worker panel */
async function addSourceToWorker(page: Page, workerNode: Locator) {
  await workerNode.click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-source-btn').click();
  await page.waitForTimeout(100);
}

/** Add a sink via the worker panel */
async function addSinkToWorker(page: Page, workerNode: Locator) {
  await workerNode.click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-sink-btn').click();
  await page.waitForTimeout(100);
}

test.describe('Node selection and property panels (PROP-01 through PROP-04, SRCE-03)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('.react-flow');
  });

  // ── Node selection visual feedback ──────────────────────────────────

  test('clicking a worker node shows visual selection indicator', async ({ page }) => {
    await addWorker(page);
    const node = page.locator('.react-flow__node').first();
    await node.click();
    await expect(node).toHaveClass(/selected/);
    const workerDiv = page.locator('.worker-node').first();
    const boxShadow = await workerDiv.evaluate(
      (el) => window.getComputedStyle(el).boxShadow,
    );
    expect(boxShadow).not.toBe('none');
  });

  test('clicking a source node shows visual selection indicator', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    // Click empty first to deselect worker, then click source
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    const sourceNode = page.locator('.react-flow__node-source');
    await sourceNode.click();
    await expect(sourceNode).toHaveClass(/selected/);
  });

  test('clicking a sink node shows visual selection indicator', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    const sinkNode = page.locator('.react-flow__node-sink');
    await sinkNode.click();
    await expect(sinkNode).toHaveClass(/selected/);
  });

  test('clicking empty canvas deselects node', async ({ page }) => {
    await addWorker(page);
    const node = page.locator('.react-flow__node').first();
    await node.click();
    await expect(node).toHaveClass(/selected/);

    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await expect(node).not.toHaveClass(/selected/);
  });

  // ── Sidebar tab switching on node click ─────────────────────────────

  test('clicking a node switches sidebar to Properties tab', async ({ page }) => {
    await page.getByRole('button', { name: 'Sources' }).click();
    await addWorker(page);
    await page.locator('.react-flow__node').first().click();
    await expect(page.getByRole('button', { name: 'Properties' })).toHaveClass(/border-blue-600/);
  });

  // ── Worker property panel (PROP-01) ─────────────────────────────────

  test('PROP-01: clicking a worker shows property panel with host, gRPC, capacity', async ({ page }) => {
    await addWorker(page);
    await page.locator('.react-flow__node').first().click();
    await expect(page.getByText('Host Address')).toBeVisible();
    await expect(page.getByText('gRPC Address')).toBeVisible();
    await expect(page.getByText('Capacity')).toBeVisible();
  });

  // ── Source property panel (PROP-02, PROP-04, SRCE-03) ───────────────

  test('PROP-02: clicking a source shows property panel with source type and config', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.source-node').click();
    await expect(page.getByText('Source Type')).toBeVisible();
    await expect(page.getByText('Source Configuration')).toBeVisible();
  });

  test('PROP-04: source panel shows type-specific config fields for Generator', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.source-node').click();
    await expect(page.getByText('Seed')).toBeVisible();
    await expect(page.getByText('Generator Schema')).toBeVisible();
  });

  test('PROP-04: changing source type updates config fields', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.source-node').click();
    const sourceTypeSelect = page.locator('select').filter({ has: page.locator('option[value="TCP"]') });
    await sourceTypeSelect.selectOption('TCP');
    await expect(page.getByText('Socket Host')).toBeVisible();
    await expect(page.getByText('Socket Port')).toBeVisible();
  });

  // ── Sink property panel (PROP-03, PROP-04) ──────────────────────────

  test('PROP-03: clicking a sink shows property panel with sink type and config', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.sink-node').click();
    await expect(page.getByText('Sink Name')).toBeVisible();
  });

  test('PROP-04: Void sink shows "no configuration required"', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.sink-node').click();
    const sinkTypeSelect = page.locator('select').filter({ has: page.locator('option[value="Void"]') });
    await sinkTypeSelect.selectOption('Void');
    await expect(page.getByText('No configuration required')).toBeVisible();
  });

  // ── Panel switching between nodes ───────────────────────────────────

  test('clicking different node types switches the panel content', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    // Click worker — should show worker panel
    await page.locator('.worker-node').click();
    await expect(page.getByText('Host Address')).toBeVisible();

    // Click source — should switch to source panel
    await page.locator('.source-node').click();
    await expect(page.getByText('Source Type')).toBeVisible();
  });

  test('clicking empty canvas shows empty state in properties panel', async ({ page }) => {
    await addWorker(page);
    await page.locator('.react-flow__node').first().click();
    await expect(page.getByText('Host Address')).toBeVisible();

    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await expect(page.getByText('Select a node to edit its properties')).toBeVisible();
  });

  // ── Navigation links between nodes ──────────────────────────────────

  test('worker panel shows clickable source link that navigates to source panel', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    // Click worker to see its panel
    await page.locator('.worker-node').click();
    await expect(page.getByText('Connected Sources')).toBeVisible();

    // Click the source link
    const sourceLink = page.locator('button').filter({ hasText: 'Generator' });
    await expect(sourceLink).toBeVisible();
    await sourceLink.click();

    // Should now show source panel
    await expect(page.getByText('Source Type')).toBeVisible();
    await expect(page.getByText('Source Configuration')).toBeVisible();
  });

  test('source panel shows host worker link that navigates to worker panel', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    // Click source to see its panel
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.source-node').click();

    await expect(page.getByText('Host Worker')).toBeVisible();
    const workerLink = page.locator('button').filter({ hasText: /worker-\d+/ });
    await expect(workerLink).toBeVisible();
    await workerLink.click();

    await expect(page.getByText('Host Address')).toBeVisible();
  });

  test('sink panel shows host worker link that navigates to worker panel', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));

    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.locator('.sink-node').click();

    await expect(page.getByText('Host Worker')).toBeVisible();
    const workerLink = page.locator('button').filter({ hasText: /worker-\d+/ });
    await expect(workerLink).toBeVisible();
    await workerLink.click();

    await expect(page.getByText('Host Address')).toBeVisible();
  });

  test('worker panel shows upstream and downstream worker sections', async ({ page }) => {
    await addWorker(page);
    await page.locator('.react-flow__node').first().click();
    await expect(page.getByText('Upstream Workers')).toBeVisible();
    await expect(page.getByText('Downstream Workers')).toBeVisible();
  });

  test('clicking a link in the property panel moves selection indicator on canvas', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    // Click worker — worker node should be selected
    await page.locator('.worker-node').click();
    const workerRfNode = page.locator('.react-flow__node-worker');
    await expect(workerRfNode).toHaveClass(/selected/);

    // Click the source link in the worker panel
    const sourceLink = page.locator('button').filter({ hasText: 'Generator' });
    await sourceLink.click();

    // Source node should now be selected, worker should not
    const sourceRfNode = page.locator('.react-flow__node-source');
    await expect(sourceRfNode).toHaveClass(/selected/);
    await expect(workerRfNode).not.toHaveClass(/selected/);
  });

  test('adding a source switches focus to the new source panel', async ({ page }) => {
    await addWorker(page);
    await page.locator('.worker-node').click();
    await expect(page.getByText('Host Address')).toBeVisible();

    // Click Add Source
    await page.getByTestId('add-source-btn').click();
    await page.waitForTimeout(200);

    // Panel should now show source properties, not worker
    await expect(page.getByText('Source Type')).toBeVisible();
    // The source node should be selected on canvas
    await expect(page.locator('.react-flow__node-source')).toHaveClass(/selected/);
    // Worker should no longer be selected
    await expect(page.locator('.react-flow__node-worker')).not.toHaveClass(/selected/);
  });

  test('adding a sink switches focus to the new sink panel', async ({ page }) => {
    await addWorker(page);
    await page.locator('.worker-node').click();
    await expect(page.getByText('Host Address')).toBeVisible();

    // Click Add Sink
    await page.getByTestId('add-sink-btn').click();
    await page.waitForTimeout(200);

    // Panel should now show sink properties
    await expect(page.getByText('Sink Name')).toBeVisible();
    // The sink node should be selected on canvas
    await expect(page.locator('.react-flow__node-sink')).toHaveClass(/selected/);
  });

  test('adding multiple sources does not stack them on top of each other', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');

    // Add two sources
    await worker.click();
    await page.getByTestId('add-source-btn').click();
    await page.waitForTimeout(100);
    // Need to re-select worker to add another (panel switched to source)
    await worker.click();
    await page.waitForTimeout(100);
    await page.getByTestId('add-source-btn').click();
    await page.waitForTimeout(200);

    // Both sources should exist
    await expect(page.locator('.source-node')).toHaveCount(2);

    // Their positions should be different
    const box0 = await page.locator('.source-node').nth(0).boundingBox();
    const box1 = await page.locator('.source-node').nth(1).boundingBox();
    expect(box0).not.toBeNull();
    expect(box1).not.toBeNull();

    // They should NOT overlap (y positions should differ by at least 40px)
    const yDiff = Math.abs(box0!.y - box1!.y);
    expect(yDiff).toBeGreaterThan(40);
  });

  test('adding multiple sinks does not stack them on top of each other', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');

    await worker.click();
    await page.getByTestId('add-sink-btn').click();
    await page.waitForTimeout(100);
    await worker.click();
    await page.waitForTimeout(100);
    await page.getByTestId('add-sink-btn').click();
    await page.waitForTimeout(200);

    await expect(page.locator('.sink-node')).toHaveCount(2);

    const box0 = await page.locator('.sink-node').nth(0).boundingBox();
    const box1 = await page.locator('.sink-node').nth(1).boundingBox();
    expect(box0).not.toBeNull();
    expect(box1).not.toBeNull();

    const yDiff = Math.abs(box0!.y - box1!.y);
    expect(yDiff).toBeGreaterThan(40);
  });

  test('no hex IDs shown in panel headings', async ({ page }) => {
    await addWorker(page);
    await page.locator('.react-flow__node').first().click();
    const heading = page.locator('h3');
    const text = await heading.textContent();
    expect(text).toBe('Worker');
  });
});
