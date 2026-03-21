import { test, expect, type Page, type Locator } from '@playwright/test';

// ── Helpers ────────────────────────────────────────────────────────────────

/** Add a worker via the toolbar button */
async function addWorker(page: Page) {
  await page.getByTestId('add-worker-btn').click();
}

/** Add a source via the worker panel's Add Source button. Worker must be selected. */
async function addSourceToWorker(page: Page, workerNode: Locator) {
  await workerNode.click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-source-btn').click();
  await page.waitForTimeout(100);
}

/** Add a sink via the worker panel's Add Sink button. Worker must be selected. */
async function addSinkToWorker(page: Page, workerNode: Locator) {
  await workerNode.click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-sink-btn').click();
  await page.waitForTimeout(100);
}

/** Wait for auto-layout to settle after structural changes */
async function waitForLayout(page: Page) {
  await page.waitForTimeout(500);
}

/** Drag a node so its center lands at the given canvas coordinates */
async function moveNode(page: Page, node: Locator, targetX: number, targetY: number) {
  const box = await node.boundingBox();
  expect(box).not.toBeNull();
  await page.mouse.move(box!.x + box!.width / 2, box!.y + box!.height / 2);
  await page.mouse.down();
  await page.mouse.move(targetX, targetY, { steps: 5 });
  await page.mouse.up();
}

/** Drag one node onto another (center-to-center) to create a connection */
async function dragNodeOnto(page: Page, src: Locator, tgt: Locator) {
  // Wait for any pending layout to settle before reading positions
  await page.waitForTimeout(300);
  const s = await src.boundingBox();
  const t = await tgt.boundingBox();
  expect(s).not.toBeNull();
  expect(t).not.toBeNull();
  await page.mouse.move(s!.x + s!.width / 2, s!.y + s!.height / 2);
  await page.mouse.down();
  // Move in more steps to ensure drag is registered
  await page.mouse.move(t!.x + t!.width / 2, t!.y + t!.height / 2, { steps: 20 });
  await page.mouse.up();
  await page.waitForTimeout(300);
}

test.describe('Interactive Canvas (CANV-01 through CANV-12)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
  });

  // ── CANV-01/02: Node creation ──────────────────────────────────────

  test('CANV-01/02: toolbar has Add Worker button', async ({ page }) => {
    await expect(page.getByTestId('add-worker-btn')).toBeVisible();
  });

  test('CANV-01/02: toolbar does not have Add Source or Add Sink buttons', async ({ page }) => {
    await expect(page.locator('.toolbar').getByText('Add Source')).not.toBeVisible();
    await expect(page.locator('.toolbar').getByText('Add Sink')).not.toBeVisible();
  });

  test('CANV-01/02: Add Worker button creates a worker node', async ({ page }) => {
    await addWorker(page);
    await expect(page.locator('.worker-node')).toHaveCount(1);
  });

  test('CANV-01/02: sources are created via worker panel and auto-attached', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    await expect(page.locator('.source-node')).toHaveCount(1);
    // Should already have an edge (auto-attached)
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  test('CANV-01/02: sinks are created via worker panel and auto-attached', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));

    await expect(page.locator('.sink-node')).toHaveCount(1);
    // Should already have an edge (auto-attached)
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  test('CANV-01/02: can create multiple sources and sinks on same worker', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');
    await addSourceToWorker(page, worker);
    await addSourceToWorker(page, worker);
    await addSinkToWorker(page, worker);

    await expect(page.locator('.source-node')).toHaveCount(2);
    await expect(page.locator('.sink-node')).toHaveCount(1);
    // 2 source edges + 1 sink edge
    await expect(page.locator('.react-flow__edge')).toHaveCount(3);
  });

  // ── CANV-03: Node repositioning ────────────────────────────────────

  test('CANV-03: nodes can be dragged to reposition', async ({ page }) => {
    await addWorker(page);
    const node = page.locator('.react-flow__node').first();
    const startBox = await node.boundingBox();
    expect(startBox).not.toBeNull();

    await node.hover();
    await page.mouse.down();
    await page.mouse.move(startBox!.x + 100, startBox!.y + 50, { steps: 5 });
    await page.mouse.up();

    const endBox = await node.boundingBox();
    expect(endBox).not.toBeNull();
    expect(Math.abs(endBox!.x - startBox!.x) + Math.abs(endBox!.y - startBox!.y)).toBeGreaterThan(20);
  });

  // ── CANV-04: Worker-to-worker connections via drag ─────────────────

  test('CANV-04: drag worker onto another worker creates downstream connection', async ({ page }) => {
    await addWorker(page);
    await addWorker(page);
    await waitForLayout(page);

    await dragNodeOnto(
      page,
      page.locator('.react-flow__node').nth(0),
      page.locator('.react-flow__node').nth(1),
    );

    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  // ── CANV-05: Source/sink auto-attachment ────────────────────────────

  test('CANV-05: source→worker edge cannot be deleted', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Try to select and delete the edge
    await page.locator('.react-flow__edge').first().click({ force: true });
    await page.keyboard.press('Delete');
    await page.waitForTimeout(200);

    // Edge should still be there
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  test('CANV-05: sink→worker edge cannot be deleted', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Try to select and delete the edge
    await page.locator('.react-flow__edge').first().click({ force: true });
    await page.keyboard.press('Delete');
    await page.waitForTimeout(200);

    // Edge should still be there
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  test('CANV-05: deleting a worker cascade-deletes its sources and sinks', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');
    await addSourceToWorker(page, worker);
    await addSinkToWorker(page, worker);

    await expect(page.locator('.react-flow__node')).toHaveCount(3);

    // Click empty canvas first to deselect, then select worker and delete
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.waitForTimeout(100);
    await worker.click();
    await page.keyboard.press('Delete');

    // All nodes and edges should be gone
    await expect(page.locator('.react-flow__node')).toHaveCount(0);
    await expect(page.locator('.react-flow__edge')).toHaveCount(0);
  });

  test('CANV-05: drag source from one worker to another re-attaches it', async ({ page }) => {
    // Create two workers and add source to first
    await addWorker(page);
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node').nth(0));
    await waitForLayout(page);
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Click source to see which worker it's attached to
    await page.locator('.source-node').click();
    await expect(page.getByText('Host Worker')).toBeVisible();
    const hostWorkerLink = page.locator('text=Host Worker').locator('..').locator('button');
    const hostBefore = await hostWorkerLink.first().textContent();

    // Deselect, then drag source onto worker2
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await dragNodeOnto(page, page.locator('.source-node'), page.locator('.worker-node').nth(1));

    // Should still have 1 edge
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Click source — host worker should have changed
    await page.locator('.source-node').click();
    await expect(page.getByText('Host Worker')).toBeVisible();
    const hostAfter = await hostWorkerLink.first().textContent();
    expect(hostAfter).not.toBe(hostBefore);
  });

  test('CANV-05: drag sink from one worker to another re-attaches it', async ({ page }) => {
    // Create two workers spread apart
    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(0), 200, 300);
    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(1), 700, 300);

    // Add sink to worker1
    await addSinkToWorker(page, page.locator('.worker-node').nth(0));
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Deselect, then move sink away
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.waitForTimeout(100);
    await moveNode(page, page.locator('.sink-node'), 900, 300);

    // Drag sink onto worker2
    await dragNodeOnto(page, page.locator('.sink-node'), page.locator('.worker-node').nth(1));
    await page.waitForTimeout(300);

    // Should still have 1 edge, now connecting to worker2
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Click sink — host worker should now be worker2
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.waitForTimeout(100);
    await page.locator('.sink-node').click();
    await expect(page.getByText('Host Worker')).toBeVisible();
    const sinkWorkerLink = page.locator('button').filter({ hasText: /worker-/ });
    await expect(sinkWorkerLink).toBeVisible();
    const sinkLinkText = await sinkWorkerLink.textContent();
    expect(sinkLinkText).not.toContain('worker-1');
  });

  // ── Full topology ──────────────────────────────────────────────────

  test('CANV-04/05: full topology — sources on worker1, downstream to worker2, sinks on worker2', async ({ page }) => {
    // Create two workers
    await addWorker(page);
    await addWorker(page);

    // Add source to worker1
    await addSourceToWorker(page, page.locator('.worker-node').nth(0));
    // Add sink to worker2
    await addSinkToWorker(page, page.locator('.worker-node').nth(1));
    await waitForLayout(page);

    // 2 edges so far (source→worker1, worker2→sink)
    await expect(page.locator('.react-flow__edge')).toHaveCount(2);

    // Connect worker1 → worker2 via drag
    await dragNodeOnto(
      page,
      page.locator('.react-flow__node').filter({ has: page.locator('.worker-node') }).nth(0),
      page.locator('.react-flow__node').filter({ has: page.locator('.worker-node') }).nth(1),
    );

    // Now 3 edges: source→worker1, worker1→worker2, worker2→sink
    await expect(page.locator('.react-flow__edge')).toHaveCount(3);
  });

  // ── CANV-06: Node and edge deletion ────────────────────────────────

  test('CANV-06: select and delete a worker with Backspace', async ({ page }) => {
    await addWorker(page);
    await expect(page.locator('.worker-node')).toHaveCount(1);

    await page.locator('.react-flow__node').first().click();
    await page.keyboard.press('Backspace');
    await expect(page.locator('.worker-node')).toHaveCount(0);
  });

  test('CANV-06: select and delete a source with Delete key', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));
    await expect(page.locator('.source-node')).toHaveCount(1);

    // Click empty to deselect, then select source
    await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
    await page.waitForTimeout(100);
    await page.locator('.source-node').click();
    await page.keyboard.press('Delete');
    await expect(page.locator('.source-node')).toHaveCount(0);
  });

  test('CANV-06: deleting a connected worker also removes its edges', async ({ page }) => {
    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(0), 400, 200);

    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(1), 400, 500);

    // Create downstream connection
    await dragNodeOnto(
      page,
      page.locator('.react-flow__node').nth(0),
      page.locator('.react-flow__node').nth(1),
    );
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Move worker1 back so we can click it cleanly
    await moveNode(page, page.locator('.react-flow__node').nth(0), 400, 200);

    // Delete worker1 — its downstream edge should also be removed
    await page.locator('.react-flow__node').nth(0).click();
    await page.keyboard.press('Delete');
    await expect(page.locator('.react-flow__edge')).toHaveCount(0);
    await expect(page.locator('.worker-node')).toHaveCount(1);
  });

  // ── CANV-07: Visual distinctness ───────────────────────────────────

  test('CANV-07: worker, source, and sink nodes are visually distinct', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');
    await addSourceToWorker(page, worker);
    await addSinkToWorker(page, worker);

    await expect(page.locator('.worker-node')).toHaveCount(1);
    await expect(page.locator('.source-node')).toHaveCount(1);
    await expect(page.locator('.sink-node')).toHaveCount(1);
  });

  test('CANV-07: worker node is larger than source and sink', async ({ page }) => {
    await addWorker(page);
    const worker = page.locator('.worker-node');
    await addSourceToWorker(page, worker);
    await addSinkToWorker(page, worker);

    const workerBox = await page.locator('.worker-node').boundingBox();
    const sourceBox = await page.locator('.source-node').boundingBox();
    const sinkBox = await page.locator('.sink-node').boundingBox();

    expect(workerBox).not.toBeNull();
    expect(sourceBox).not.toBeNull();
    expect(sinkBox).not.toBeNull();

    expect(workerBox!.width).toBeGreaterThan(sourceBox!.width);
    expect(workerBox!.width).toBeGreaterThan(sinkBox!.width);
  });

  test('CANV-07: worker node displays host address', async ({ page }) => {
    await addWorker(page);
    await expect(page.locator('.worker-node__label')).toBeVisible();
    await expect(page.locator('.worker-node__label')).toContainText(':');
  });

  test('CANV-07: sink node has amber/orange border', async ({ page }) => {
    await addWorker(page);
    await addSinkToWorker(page, page.locator('.worker-node'));
    const borderColor = await page.locator('.sink-node').evaluate(
      (el) => getComputedStyle(el).borderColor,
    );
    expect(borderColor).toBe('rgb(245, 158, 11)');
  });

  // ── CANV-08: Connection validation ─────────────────────────────────

  test('CANV-08: cycle-creating connection is rejected (A→B then B→A)', async ({ page }) => {
    await addWorker(page);
    await addWorker(page);
    await waitForLayout(page);

    // Connect A→B
    await dragNodeOnto(
      page,
      page.locator('.react-flow__node').nth(0),
      page.locator('.react-flow__node').nth(1),
    );
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);

    // Try B→A (cycle) — should be rejected
    await dragNodeOnto(
      page,
      page.locator('.react-flow__node').nth(1),
      page.locator('.react-flow__node').nth(0),
    );
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
  });

  test('CANV-08: prevents cycle in worker connections (A→B→C→A)', async ({ page }) => {
    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(0), 500, 100);

    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(1), 500, 400);

    await addWorker(page);
    await moveNode(page, page.locator('.worker-node').nth(2), 500, 700);

    const nodes = page.locator('.react-flow__node');

    // A→B
    await dragNodeOnto(page, nodes.nth(0), nodes.nth(1));
    await expect(page.locator('.react-flow__edge')).toHaveCount(1);
    await moveNode(page, nodes.nth(0), 500, 100);

    // B→C
    await dragNodeOnto(page, nodes.nth(1), nodes.nth(2));
    await expect(page.locator('.react-flow__edge')).toHaveCount(2);
    await moveNode(page, nodes.nth(1), 500, 400);

    // Try C→A (cycle) — should be rejected
    await dragNodeOnto(page, nodes.nth(2), nodes.nth(0));
    await expect(page.locator('.react-flow__edge')).toHaveCount(2);
  });

  // ── CANV-09: Grid snapping ─────────────────────────────────────────

  test('CANV-09: grid background with dots is rendered', async ({ page }) => {
    await expect(page.locator('.react-flow')).toBeVisible();
    await expect(page.locator('.react-flow__background')).toBeVisible();
  });

  // ── CANV-10: Minimap ───────────────────────────────────────────────

  test('CANV-10: minimap is visible', async ({ page }) => {
    await expect(page.locator('.react-flow__minimap')).toBeVisible();
  });

  test('CANV-10: minimap updates when nodes are added', async ({ page }) => {
    const initialRects = await page.locator('.react-flow__minimap-node').count();

    await addWorker(page);
    await addSourceToWorker(page, page.locator('.worker-node'));

    await page.waitForTimeout(500);
    const finalRects = await page.locator('.react-flow__minimap-node').count();
    expect(finalRects).toBeGreaterThan(initialRects);
  });

  // ── CANV-12: Copy/paste ────────────────────────────────────────────

  test('CANV-12: copy and paste a selected node with Ctrl+C/V', async ({ page }) => {
    await addWorker(page);
    await expect(page.locator('.worker-node')).toHaveCount(1);

    await page.locator('.react-flow__node').first().click();
    await page.keyboard.press('Control+c');
    await page.keyboard.press('Control+v');

    await expect(page.locator('.worker-node')).toHaveCount(2);
  });

  // ── Handle dots are hidden ─────────────────────────────────────────

  test('connection handles are hidden from the user', async ({ page }) => {
    await addWorker(page);
    const handle = page.locator('.react-flow__handle').first();
    await expect(handle).toBeAttached();
    const opacity = await handle.evaluate((el) => getComputedStyle(el).opacity);
    expect(opacity).toBe('0');
  });

  // ── Stability ──────────────────────────────────────────────────────

  test('no infinite loop: page loads without errors', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(err.message));

    await page.goto('/');
    await page.waitForSelector('.react-flow');
    await page.waitForTimeout(2000);

    const loopErrors = errors.filter(
      (e) => e.includes('Maximum update depth') || e.includes('getSnapshot'),
    );
    expect(loopErrors).toHaveLength(0);
  });

  test('no errors after adding and deleting multiple nodes', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(err.message));

    await addWorker(page);
    await addWorker(page);

    await expect(page.locator('.react-flow__node')).toHaveCount(2);

    for (let i = 0; i < 2; i++) {
      await page.locator('.react-flow__pane').click({ position: { x: 50, y: 50 } });
      await page.waitForTimeout(100);
      const node = page.locator('.react-flow__node').first();
      await node.click();
      await page.waitForTimeout(100);
      await page.keyboard.press('Delete');
      await page.waitForTimeout(200);
    }
    await expect(page.locator('.react-flow__node')).toHaveCount(0);

    expect(errors).toHaveLength(0);
  });
});
